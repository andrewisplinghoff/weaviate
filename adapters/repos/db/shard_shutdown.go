//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package db

import (
	"context"
	"fmt"
	"time"

	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/entities/errorcompounder"
	"github.com/weaviate/weaviate/entities/storagestate"
)

/*

	batch
		shut
		false
			in_use ++
			defer in_use --
		true
			fail request

	shutdown
		loop + time:
		if shut == true
			fail request
		in_use == 0 && shut == false
			shut = true

*/
// Shutdown needs to be idempotent, so it can also deal with a partial
// initialization. In some parts, it relies on the underlying structs to have
// idempotent Shutdown methods. In other parts, it explicitly checks if a
// component was initialized. If not, it turns it into a noop to prevent
// blocking.
func (s *Shard) Shutdown(ctx context.Context) (err error) {
	start := time.Now()
	defer func() {
		s.index.metrics.ObserveUpdateShardStatus(storagestate.StatusShutdown.String(), time.Since(start))

		if err != nil {
			return
		}

		s.UpdateStatus(storagestate.StatusShutdown.String())
	}()

	s.reindexer.Stop(s, fmt.Errorf("shard shutdown"))

	if err = s.waitForShutdown(ctx); err != nil {
		return
	}

	s.haltForTransferMux.Lock()
	if s.haltForTransferCancel != nil {
		s.haltForTransferCancel()
	}
	s.haltForTransferMux.Unlock()

	ec := errorcompounder.New()

	err = s.GetPropertyLengthTracker().Close()
	ec.AddWrap(err, "close prop length tracker")

	// unregister all callbacks at once, in parallel
	err = cyclemanager.NewCombinedCallbackCtrl(0, s.index.logger,
		s.cycleCallbacks.compactionCallbacksCtrl,
		s.cycleCallbacks.compactionAuxCallbacksCtrl,
		s.cycleCallbacks.flushCallbacksCtrl,
		s.cycleCallbacks.vectorCombinedCallbacksCtrl,
		s.cycleCallbacks.geoPropsCombinedCallbacksCtrl,
	).Unregister(ctx)
	ec.Add(err)

	s.mayStopAsyncReplication()

	_ = s.ForEachVectorQueue(func(targetVector string, queue *VectorIndexQueue) error {
		if err = queue.Flush(); err != nil {
			ec.Add(fmt.Errorf("flush vector index queue commitlog of vector %q: %w", targetVector, err))
		}

		if err = queue.Close(); err != nil {
			ec.Add(fmt.Errorf("shut down vector index queue of vector %q: %w", targetVector, err))
		}

		return nil
	})

	_ = s.ForEachVectorIndex(func(targetVector string, index VectorIndex) error {
		// to ensure that all commitlog entries are written to disk.
		// otherwise in some cases the tombstone cleanup process'
		// 'RemoveTombstone' entry is not picked up on restarts
		// resulting in perpetually attempting to remove a tombstone
		// which doesn't actually exist anymore
		if err = index.Flush(); err != nil {
			ec.Add(fmt.Errorf("flush vector index commitlog of vector %q: %w", targetVector, err))
		}

		if err = index.Shutdown(ctx); err != nil {
			ec.Add(fmt.Errorf("shut down vector index of vector %q: %w", targetVector, err))
		}

		return nil
	})

	if s.store != nil {
		// store would be nil if loading the objects bucket failed, as we would
		// only return the store on success from s.initLSMStore()
		err = s.store.Shutdown(ctx)
		ec.AddWrap(err, "stop lsmkv store")
	}

	if s.dynamicVectorIndexDB != nil {
		err = s.dynamicVectorIndexDB.Close()
		ec.AddWrap(err, "stop dynamic vector index db")
	}

	if s.dimensionTrackingInitialized.Load() {
		// tracking vector dimensions goroutine only works when tracking is enabled
		// _and_ when initialization completed, that's why we are trying to stop it
		// only in this case
		s.stopDimensionTracking <- struct{}{}
	}

	return ec.ToError()
}

func (s *Shard) preventShutdown() (release func(), err error) {
	s.shutdownLock.RLock()
	defer s.shutdownLock.RUnlock()

	if s.shut {
		return func() {}, errAlreadyShutdown
	}

	s.inUseCounter.Add(1)
	return func() { s.inUseCounter.Add(-1) }, nil
}

func (s *Shard) waitForShutdown(ctx context.Context) error {
	checkInterval := 50 * time.Millisecond
	timeout := 30 * time.Second

	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	if eligible, err := s.checkEligibleForShutdown(); err != nil {
		return err
	} else if !eligible {
		ticker := time.NewTicker(checkInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return fmt.Errorf("Shard::proceedWithShutdown: %w", ctx.Err())
			case <-ticker.C:
				if eligible, err := s.checkEligibleForShutdown(); err != nil {
					return err
				} else if eligible {
					return nil
				}
			}
		}
	}
	return nil
}

// checks whether shutdown can be executed
// (shard is not in use at the moment)
func (s *Shard) checkEligibleForShutdown() (eligible bool, err error) {
	s.shutdownLock.Lock()
	defer s.shutdownLock.Unlock()

	if s.shut {
		return false, errAlreadyShutdown
	}

	if s.inUseCounter.Load() == 0 {
		s.shut = true
		return true, nil
	}

	return false, nil
}

// // cleanupPartialInit is called when the shard was only partially initialized.
// // Internally it just uses [Shutdown], but also adds some logging.
// func (s *Shard) cleanupPartialInit(ctx context.Context) {
// 	log := s.index.logger.WithField("action", "cleanup_partial_initialization")
// 	if err := s.Shutdown(ctx); err != nil {
// 		log.WithError(err).Error("failed to shutdown store")
// 	}

// 	log.Debug("successfully cleaned up partially initialized shard")
// }

// func (s *Shard) NotifyReady() {
// 	s.initStatus()
// 	s.index.logger.
// 		WithField("action", "startup").
// 		Debugf("shard=%s is ready", s.name)
// }
