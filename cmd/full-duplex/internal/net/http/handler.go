package http

import (
	"io"
	"net/http"
)

func Handler() http.Handler {
	mux := http.NewServeMux()
	mux.Handle("POST /map", handleMap())
	return mux
}

func handleMap() HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) error {
		rc := http.NewResponseController(w)
		if err := rc.EnableFullDuplex(); err != nil {
			return StatusCode(http.StatusNotImplemented)
		}
		_, err := io.Copy(w, r.Body)
		return err
	}
}

type HandlerFunc func(w http.ResponseWriter, r *http.Request) error

func (h HandlerFunc) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	err := h(w, r)
	if err == nil {
		return
	}
	if h, ok := err.(http.Handler); ok {
		h.ServeHTTP(w, r)
		return
	}
}

type StatusCode int

func (e StatusCode) Error() string { return http.StatusText(int(e)) }

func (e StatusCode) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(int(e))
}
