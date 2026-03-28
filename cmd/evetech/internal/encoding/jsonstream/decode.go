package jsonstream

import (
	"encoding/json"
	"io"
	"iter"
)

func Decode[T any](r io.Reader, stream bool) iter.Seq2[T, error] {
	// src/encoding/json/example_test.go
	d := json.NewDecoder(r)
	var v T
	return func(yield func(T, error) bool) {
		if !stream {
			if _, err := d.Token(); err != nil && !yield(v, err) {
				return
			}
		}
		for {
			if !stream && !d.More() {
				break
			}
			err := d.Decode(&v)
			if stream && err == io.EOF {
				break
			}
			if !yield(v, err) {
				return
			}
			if err != nil && !stream {
				return
			}
		}
		if !stream {
			if _, err := d.Token(); err != nil && !yield(v, err) {
				return
			}
		}
	}
}
