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
	val := GetEnvOrDefaultStr(key, defaultVal)
	items := strings.Split(val, separator)
	result := make([]string, len(items))
	for i := 0; i < len(items); i++ {
		result[i] = strings.TrimSpace(items[i])
	}
	return result
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
