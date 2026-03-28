package http

import (
	"io"
	"net/http"
	"net/url"
	"runtime/trace"

	"github.com/adoublef/bench/cmd/evetech/internal/order"
)

func Handler(h *order.Handler) http.Handler {
	return handleCSV(h)
}

func handleCSV(h *order.Handler) HandlerFunc {
	parse := func(_ http.ResponseWriter, r *http.Request) (base *url.URL, hasHeader bool, err error) {
		u, err := url.Parse(r.URL.Query().Get("base_url"))
		return u, false, err
	}

	return func(w http.ResponseWriter, r *http.Request) error {
		ctx, task := trace.NewTask(r.Context(), "handleFunc")
		defer task.End()

		u, has, err := parse(w, r)
		if err != nil {
			return err
		}

		s := h.OrderStream(ctx, u, has)
		defer s.Close()

		h := w.Header()
		h.Set("Content-Type", "text/csv")
		h.Set("Content-Disposition", "attachment; filename=\"evetech.csv\"")

		_, err = io.CopyBuffer(w, s, nil)
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
