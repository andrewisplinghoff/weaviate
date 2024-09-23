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

package segmentindex

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"

	"github.com/sirupsen/logrus"

	"github.com/pkg/errors"
)

type Indexes struct {
	Keys                []Key
	SecondaryIndexCount uint16
	ScratchSpacePath    string
	Logger              logrus.FieldLogger
}

func mapEntriesToStrings(dirPath string, entries []os.DirEntry) []string {
	result := make([]string, len(entries))
	for i, entry := range entries {
		info, err := entry.Info()
		if err != nil {
			result[i] = fmt.Sprintf("%s (error retrieving size)", entry.Name())
			continue
		}

		filePath := filepath.Join(dirPath, entry.Name())
		// Integrate the checkFileAccess call directly here with improved error handling
		cmd := exec.Command("sh", "-c", fmt.Sprintf("lsof | grep '%s'", filePath))
		var out bytes.Buffer
		cmd.Stdout = &out
		cmd.Stderr = &out

		// Run the command and capture output
		err = cmd.Run()
		var processes string
		if err != nil {
			if out.Len() == 0 {
				processes = "none" // No processes are accessing the file
			} else {
				processes = fmt.Sprintf("error: %v", err)
			}
		} else {
			processes = strings.TrimSpace(out.String())
			if processes == "" {
				processes = "none" // No processes found in output
			}
		}

		result[i] = fmt.Sprintf("%s (%d bytes, accessed by: %s)", entry.Name(), info.Size(), processes)
	}
	return result
}

func (s Indexes) WriteTo(w io.Writer) (int64, error) {
	var currentOffset uint64 = HeaderSize
	if len(s.Keys) > 0 {
		currentOffset = uint64(s.Keys[len(s.Keys)-1].ValueEnd)
	}
	var written int64

	if _, err := os.Stat(s.ScratchSpacePath); err == nil {
		// exists, we need to delete
		// This could be the case if Weaviate shut down unexpectedly (i.e. crashed)
		// while a compaction was running. We can safely discard the contents of
		// the scratch space.

		if err := os.RemoveAll(s.ScratchSpacePath); err != nil {
			s.Logger.WithError(err).WithField("ScratchSpacePath", s.ScratchSpacePath).Errorf("os.RemoveAll(s.ScratchSpacePath) failed")
			return written, errors.Wrap(err, "clean up previous scratch space")
		}
	} else if os.IsNotExist(err) {
		// does not exist yet, nothing to - will be created in the next step
	} else {
		s.Logger.WithError(err).WithField("ScratchSpacePath", s.ScratchSpacePath).Errorf("os.Stat(s.ScratchSpacePath) failed")
		return written, errors.Wrap(err, "check for scratch space directory")
	}

	if err := os.Mkdir(s.ScratchSpacePath, 0o777); err != nil {
		s.Logger.WithError(err).WithField("ScratchSpacePath", s.ScratchSpacePath).Errorf("os.Mkdir(s.ScratchSpacePath, 0o777) failed")
		return written, errors.Wrap(err, "create scratch space")
	}

	entries, err := os.ReadDir(s.ScratchSpacePath)
	if err != nil {
		s.Logger.WithError(err).WithField("ScratchSpacePath", s.ScratchSpacePath).Errorf("Failed to read file entries at start (empty scratch space)")
	} else {
		s.Logger.WithField("ScratchSpacePath", s.ScratchSpacePath).Debugf("Entries read at start (empty scratch space): %s", mapEntriesToStrings(s.ScratchSpacePath, entries))
	}

	primaryFileName := filepath.Join(s.ScratchSpacePath, "primary")
	primaryFD, err := os.Create(primaryFileName)
	if err != nil {
		s.Logger.WithError(err).WithField("ScratchSpacePath", s.ScratchSpacePath).Errorf("os.Create(primaryFileName) failed")
		return written, err
	}

	primaryFDBuffered := bufio.NewWriter(primaryFD)

	n, err := s.buildAndMarshalPrimary(primaryFDBuffered, s.Keys)
	if err != nil {
		s.Logger.WithError(err).WithField("ScratchSpacePath", s.ScratchSpacePath).Errorf("s.buildAndMarshalPrimary(primaryFDBuffered, s.Keys) failed")
		return written, err
	}

	if err := primaryFDBuffered.Flush(); err != nil {
		s.Logger.WithError(err).WithField("ScratchSpacePath", s.ScratchSpacePath).Errorf("primaryFDBuffered.Flush() failed")
		return written, err
	}

	primaryFD.Seek(0, io.SeekStart)

	// pretend that primary index was already written, then also account for the
	// additional offset pointers (one for each secondary index)
	currentOffset = currentOffset + uint64(n) +
		uint64(s.SecondaryIndexCount)*8

	// secondaryIndicesBytes := bytes.NewBuffer(nil)
	secondaryFileName := filepath.Join(s.ScratchSpacePath, "secondary")
	secondaryFD, err := os.Create(secondaryFileName)
	if err != nil {
		s.Logger.WithError(err).WithField("ScratchSpacePath", s.ScratchSpacePath).Errorf("os.Create(secondaryFileName) failed")
		return written, err
	}

	secondaryFDBuffered := bufio.NewWriter(secondaryFD)

	if s.SecondaryIndexCount > 0 {
		offsets := make([]uint64, s.SecondaryIndexCount)
		for pos := range offsets {
			n, err := s.buildAndMarshalSecondary(secondaryFDBuffered, pos, s.Keys)
			if err != nil {
				s.Logger.WithError(err).WithField("ScratchSpacePath", s.ScratchSpacePath).Errorf("s.buildAndMarshalSecondary(secondaryFDBuffered, pos, s.Keys) failed")
				return written, err
			} else {
				written += int64(n)
			}

			offsets[pos] = currentOffset
			currentOffset = offsets[pos] + uint64(n)
		}

		if err := binary.Write(w, binary.LittleEndian, &offsets); err != nil {
			s.Logger.WithError(err).WithField("ScratchSpacePath", s.ScratchSpacePath).Errorf("binary.Write(w, binary.LittleEndian, &offsets) failed")
			return written, err
		}

		written += int64(len(offsets)) * 8
	}

	if err := secondaryFDBuffered.Flush(); err != nil {
		s.Logger.WithError(err).WithField("ScratchSpacePath", s.ScratchSpacePath).Errorf("secondaryFDBuffered.Flush() failed")
		return written, err
	}

	secondaryFD.Seek(0, io.SeekStart)

	if n, err := io.Copy(w, primaryFD); err != nil {
		s.Logger.WithError(err).WithField("ScratchSpacePath", s.ScratchSpacePath).Errorf("io.Copy(w, primaryFD) failed")
		return written, err
	} else {
		written += int64(n)
	}

	if n, err := io.Copy(w, secondaryFD); err != nil {
		s.Logger.WithError(err).WithField("ScratchSpacePath", s.ScratchSpacePath).Errorf("io.Copy(w, secondaryFD) failed")
		return written, err
	} else {
		written += int64(n)
	}

	entries, err = os.ReadDir(s.ScratchSpacePath)
	if err != nil {
		s.Logger.WithError(err).WithField("ScratchSpacePath", s.ScratchSpacePath).Errorf("Failed to read file entries before close")
	} else {
		s.Logger.WithField("ScratchSpacePath", s.ScratchSpacePath).Debugf("Before closing of files: %s", mapEntriesToStrings(s.ScratchSpacePath, entries))
	}

	if err := primaryFD.Close(); err != nil {
		s.Logger.WithError(err).WithField("ScratchSpacePath", s.ScratchSpacePath).Errorf("primaryFD.Close() failed")
		return written, err
	}

	if err := secondaryFD.Close(); err != nil {
		s.Logger.WithError(err).WithField("ScratchSpacePath", s.ScratchSpacePath).Errorf("secondaryFD.Close() failed")
		return written, err
	}

	entries, err = os.ReadDir(s.ScratchSpacePath)
	if err != nil {
		s.Logger.WithError(err).WithField("ScratchSpacePath", s.ScratchSpacePath).Errorf("Failed to read file entries after close, before remove")
	} else {
		s.Logger.WithField("ScratchSpacePath", s.ScratchSpacePath).Debugf("Entries read after close, before remove: %s", mapEntriesToStrings(s.ScratchSpacePath, entries))
	}

	if err := os.Remove(primaryFileName); err != nil {
		return written, fmt.Errorf("error %w while removing primaryFileName %s", err, primaryFileName)
	}

	if err := os.Remove(secondaryFileName); err != nil {
		return written, fmt.Errorf("error %w while removing secondaryFileName %s", err, secondaryFileName)
	}

	entries, err = os.ReadDir(s.ScratchSpacePath)
	if err != nil {
		s.Logger.WithError(err).WithField("ScratchSpacePath", s.ScratchSpacePath).Errorf("Failed to read file entries after remove")
	} else {
		s.Logger.WithField("ScratchSpacePath", s.ScratchSpacePath).Debugf("Entries read after remove: %s", mapEntriesToStrings(s.ScratchSpacePath, entries))
	}

	if err := os.RemoveAll(s.ScratchSpacePath); err != nil {
		entries, err2 := os.ReadDir(s.ScratchSpacePath)
		if err2 != nil {
			return written, fmt.Errorf("RemoveAll() at end of LSM WriteTo() failed with error %w, %w while reading file entries", err, err2)
		}
		s.Logger.WithError(err).WithField("ScratchSpacePath", s.ScratchSpacePath).Errorf("RemoveAll() at end of LSM WriteTo() failed, entries read: %s", mapEntriesToStrings(s.ScratchSpacePath, entries))
		return written, err
	}

	return written, nil
}

func checkFileAccess(filePath string) (string, error) {
	// Prepare the `lsof | grep <filename>` command
	cmd := exec.Command("sh", "-c", fmt.Sprintf("lsof | grep '%s'", filePath))
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &out

	// Run the command and capture output
	err := cmd.Run()
	if err != nil && out.Len() == 0 {
		return "", err
	}

	return strings.TrimSpace(out.String()), nil
}

// pos indicates the position of a secondary index, assumes unsorted keys and
// sorts them
func (s *Indexes) buildAndMarshalSecondary(w io.Writer, pos int,
	keys []Key,
) (int64, error) {
	keyNodes := make([]Node, len(keys))
	i := 0
	for _, key := range keys {
		if pos >= len(key.SecondaryKeys) {
			// a secondary key is not guaranteed to be present. For example, a delete
			// operation could pe performed using only the primary key
			continue
		}

		keyNodes[i] = Node{
			Key:   key.SecondaryKeys[pos],
			Start: uint64(key.ValueStart),
			End:   uint64(key.ValueEnd),
		}
		i++
	}

	keyNodes = keyNodes[:i]

	sort.Slice(keyNodes, func(a, b int) bool {
		return bytes.Compare(keyNodes[a].Key, keyNodes[b].Key) < 0
	})

	index := NewBalanced(keyNodes)
	n, err := index.MarshalBinaryInto(w)
	if err != nil {
		return 0, err
	}

	return n, nil
}

// assumes sorted keys and does NOT sort them again
func (s *Indexes) buildAndMarshalPrimary(w io.Writer, keys []Key) (int64, error) {
	keyNodes := make([]Node, len(keys))
	for i, key := range keys {
		keyNodes[i] = Node{
			Key:   key.Key,
			Start: uint64(key.ValueStart),
			End:   uint64(key.ValueEnd),
		}
	}
	index := NewBalanced(keyNodes)

	n, err := index.MarshalBinaryInto(w)
	if err != nil {
		return -1, err
	}

	return n, nil
}
