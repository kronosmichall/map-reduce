package main

import (
	"strconv"
	"strings"
)

// Example map function for word count
func wordCountMap(filename string, contents string) []KeyValue {
	// Split contents into words
	words := strings.Fields(contents)
	var kvPairs []KeyValue

	for _, word := range words {
		// Clean the word (remove punctuation, convert to lowercase)
		cleaned := strings.ToLower(strings.Trim(word, ".,!?;:\"'()[]{}"))
		if cleaned != "" {
			kvPairs = append(kvPairs, KeyValue{Key: cleaned, Value: "1"})
		}
	}

	return kvPairs
}

// Example reduce function for word count
func wordCountReduce(key string, values []string) string {
	count := 0
	for _, value := range values {
		if n, err := strconv.Atoi(value); err == nil {
			count += n
		}
	}
	return strconv.Itoa(count)
}
