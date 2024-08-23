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

package ent

import (
	"github.com/sirupsen/logrus"
	"net/http"
	"strconv"
	"time"

	"github.com/weaviate/weaviate/usecases/modulecomponents"
)

const dummyLimit = 10000000

func GetRateLimitsFromHeader(header http.Header, logger logrus.FieldLogger) *modulecomponents.RateLimits {
	requestsReset, err := time.ParseDuration(header.Get("x-ratelimit-reset-requests"))
	if err != nil {
		requestsReset = time.Duration(getHeaderInt(header, "x-ratelimit-reset-requests")) * time.Second
		logger.WithField("action", "batch_worker").
			WithField("requestsReset", requestsReset).
			Info("requestsReset was not a duration, interpreted as number of seconds")
		if err != nil {
			requestsReset = 0
		}
	}
	tokensReset, err := time.ParseDuration(header.Get("x-ratelimit-reset-tokens"))
	if err != nil {
		tokensReset = time.Duration(getHeaderInt(header, "x-ratelimit-reset-tokens")) * time.Second
		logger.WithField("action", "batch_worker").
			WithField("tokensReset", tokensReset).
			Info("tokensReset was not a duration, interpreted as number of seconds")
		if err != nil {
			tokensReset = 0
		}
	}
	limitRequests := getHeaderInt(header, "x-ratelimit-limit-requests")
	limitTokens := getHeaderInt(header, "x-ratelimit-limit-tokens")
	remainingRequests := getHeaderInt(header, "x-ratelimit-remaining-requests")
	remainingTokens := getHeaderInt(header, "x-ratelimit-remaining-tokens")

	logger.WithField("action", "batch_worker").
		WithField("requestsReset", requestsReset).
		WithField("tokensReset", tokensReset).
		WithField("limitRequests", limitRequests).
		WithField("limitTokens", limitTokens).
		WithField("remainingRequests", remainingRequests).
		WithField("remainingTokens", remainingTokens).
		WithField("time_requests_reset", time.Now().Add(requestsReset)).
		WithField("time_tokens_reset", time.Now().Add(tokensReset)).
		Info("Initial rate limits from header")

	// azure returns 0 as limit, make sure this does not block anything by setting a high value
	if limitTokens == 0 {
		limitTokens = dummyLimit
	}
	if limitRequests == 0 {
		limitRequests = dummyLimit
	}

	logger.WithField("action", "batch_worker").
		WithField("limitRequests", limitRequests).
		WithField("limitTokens", limitTokens).
		Info("After applying dummy limits")

	return &modulecomponents.RateLimits{
		LimitRequests:     limitRequests,
		LimitTokens:       limitTokens,
		RemainingRequests: remainingRequests,
		RemainingTokens:   remainingTokens,
		ResetRequests:     time.Now().Add(requestsReset),
		ResetTokens:       time.Now().Add(tokensReset),
	}
}

func getHeaderInt(header http.Header, key string) int {
	value := header.Get(key)
	if value == "" {
		return 0
	}
	i, err := strconv.Atoi(value)
	if err != nil {
		return 0
	}
	return i
}
