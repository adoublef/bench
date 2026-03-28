package http

import (
	"cmp"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"runtime/trace"
	"strconv"

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

func get(ctx context.Context, httpC *http.Client, format string, v ...any) (io.ReadCloser, error) {
	req, err1 := http.NewRequestWithContext(ctx, http.MethodGet, fmt.Sprintf(format, v...), nil)
	res, err2 := httpC.Do(req)
	if err := cmp.Or(err1, err2); err != nil {
		return nil, err
	}
	if c := res.StatusCode; c != http.StatusOK {
		_ = res.Body.Close()
		return nil, StatusCode(c)
	}
	return res.Body, nil
}

func max(ctx context.Context, httpC *http.Client, format string, v ...any) (uint64, error) {
	req, err1 := http.NewRequestWithContext(ctx, http.MethodHead, fmt.Sprintf(format, v...), nil)
	res, err2 := httpC.Do(req)
	if err := cmp.Or(err1, err2); err != nil {
		return 0, err
	}
	defer res.Body.Close()
	if c := res.StatusCode; c != http.StatusOK {
		return 0, StatusCode(c)
	}
	return strconv.ParseUint(res.Header.Get("x-pages"), 10, 32)
}
