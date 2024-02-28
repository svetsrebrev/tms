package utils

import (
	"context"
	"errors"
	"os"
	"strconv"
	"strings"

	"github.com/rs/zerolog/log"
)

func LogIfNotCancelled(err error, msg string) {
	if err != nil && !errors.Is(err, context.Canceled) {
		log.Error().Err(err).Msg(msg)
	}
}

func CollectAll[A, B any](collection []A, fn func(A) B) []B {
	res := make([]B, 0, len(collection))
	for _, item := range collection {
		res = append(res, fn(item))
	}
	return res
}

func GetEnvOrDefaultStr(key string, defaultVal string) string {
	return getEnvOrDefault(key, defaultVal, noOp)
}

func GetEnvOrDefaultInt(key string, defaultVal int) int {
	return getEnvOrDefault(key, defaultVal, strconv.Atoi)
}

func GetEnvOrDefaultArray(key, defaultVal, separator string) []string {
	return getEnvOrDefaultArray(key, defaultVal, separator, noOp)
}

func GetEnvOrDefaultIntArray(key, defaultVal, separator string) []int {
	return getEnvOrDefaultArray(key, defaultVal, separator, strconv.Atoi)
}

func noOp(v string) (string, error) { return v, nil }

func getEnvOrDefault[T any](key string, defaultVal T, convert func(string) (T, error)) T {
	val := os.Getenv(key)
	if val == "" {
		log.Warn().Msgf("%s not set, defaulting to '%v'", key, defaultVal)
		return defaultVal
	}

	converted, err := convert(val)
	if err != nil {
		log.Warn().Msgf("%s is not an %T, defaulting to '%v'", key, *new(T), defaultVal)
		return defaultVal
	}
	return converted
}

func getEnvOrDefaultArray[T any](key string, defaultVal, separator string, convert func(string) (T, error)) []T {
	val := getEnvOrDefault(key, defaultVal, noOp)
	items := strings.Split(val, separator)
	result := make([]T, 0, len(items))
	for _, item := range items {
		item = strings.TrimSpace(item)
		converted, err := convert(item)
		if err == nil {
			result = append(result, converted)
		} else {
			log.Warn().Msgf("%s is not an %T, skipping it", key, *new(T))
		}
	}
	return result
}
