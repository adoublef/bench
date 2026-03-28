package jsonstream

import (
	"encoding/json"
	"io"
	"iter"
)

func Decode[T any](r io.Reader, stream bool) iter.Seq2[T, error] {
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
			if err != nil {
				if !stream {
					return
				}
			}
		}
		if !stream {
			if _, err := d.Token(); err != nil && !yield(v, err) {
				return
			}
		}
	}
}

/*
if !stream {
			if _, err := d.Token(); err != nil {
				yield(v, err)
				return
			}
		}
		for {
			if !stream && !d.More() { // array
				break
			}
			err := d.Decode(&v)
			if stream && errors.Is(err, io.EOF) {
				break
			}
			if !yield(v, err) {
				return
			}
			if err != nil { // In array mode, if we hit EOF early we'll let the next iteration or the closing token logic handle it.
				if !stream { // For array mode, propagate error and stop
					return
				}
			}
		}
		if !stream {
			if _, err := d.Token(); err != nil {
				yield(v, err)
				return
			}
		}
*/
