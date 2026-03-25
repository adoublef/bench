package jsonstream

import (
	"encoding/json"
	"io"
	"iter"
)

func Decode[T any](r io.Reader) iter.Seq2[T, error] {
	d := json.NewDecoder(r)
	var v T
	return func(yield func(T, error) bool) {
		if _, err := d.Token(); err != nil && !yield(v, err) {
			return
		}
		for d.More() {
			if err := d.Decode(&v); !yield(v, err) {
				return
			}
		}
		if _, err := d.Token(); err != nil && !yield(v, err) {
			return
		}
	}
}
