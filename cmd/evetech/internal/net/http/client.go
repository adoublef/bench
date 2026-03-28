package http

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"iter"
	"net/http"
	"net/url"
	"strconv"

	"github.com/adoublef/bench/cmd/evetech/internal/encoding/jsonstream"
	"github.com/adoublef/bench/cmd/evetech/internal/order"
)

type Client struct{ *http.Client }

func (c *Client) Regions(ctx context.Context, u *url.URL) iter.Seq2[uint64, error] {
	url := fmt.Sprintf("%s://%s/v1/universe/regions", u.Scheme, u.Host)
	return func(yield func(uint64, error) bool) {
		req, err1 := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		res, err2 := c.Client.Do(req)
		if err := cmp.Or(err1, err2); err != nil {
			yield(0, err)
			return
		}
		defer res.Body.Close()
		if c := res.StatusCode; c != http.StatusOK {
			yield(0, StatusCode(c))
			return
		}
		var stream bool
		switch res.Header.Get("Content-Type") {
		case "application/json":
		case "application/x-ndjson", "application/jsonl":
			stream = true
		default:
			yield(0, errors.New("invalid content type"))
			return
		}
		for id, err := range jsonstream.Decode[uint64](res.Body, stream) {
			if !yield(id, err) {
				return
			}
		}
	}
}

func (c *Client) Max(ctx context.Context, u *url.URL, region uint64) (uint64, error) {
	url := fmt.Sprintf("%s://%s/v1/markets/%d/orders", u.Scheme, u.Host, region)
	req, err1 := http.NewRequestWithContext(ctx, http.MethodHead, url, nil)
	res, err2 := c.Client.Do(req)
	if err := cmp.Or(err1, err2); err != nil {
		return 0, err
	}
	defer res.Body.Close()
	if c := res.StatusCode; c != http.StatusOK {
		return 0, StatusCode(c)
	}
	return strconv.ParseUint(res.Header.Get("x-pages"), 10, 32)
}

func (c *Client) Orders(ctx context.Context, u *url.URL, region, page uint64) iter.Seq2[order.Order, error] {
	url := fmt.Sprintf("%s://%s/v1/markets/%d/orders?page=%d", u.Scheme, u.Host, region, page)
	var o order.Order
	// https://sinclairtarget.com/blog/2025/07/error-handling-with-iterators-in-go/
	// https://funnelstory.ai/blog/engineering/practical-patterns-for-go-iterators
	// https://go.dev/play/p/6qhH6ZN6f0S
	// https://go.dev/play/p/55w9IqH0KPQ
	return func(yield func(order.Order, error) bool) {
		req, err1 := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		res, err2 := c.Client.Do(req)
		if err := cmp.Or(err1, err2); err != nil {
			yield(o, err)
			return
		}
		defer res.Body.Close()
		if c := res.StatusCode; c != http.StatusOK {
			yield(o, StatusCode(c))
			return
		}
		var stream bool
		switch res.Header.Get("Content-Type") {
		case "application/json":
		case "application/x-ndjson", "application/jsonl":
			stream = true
		default:
			yield(o, errors.New("invalid content type"))
			return
		}
		for o, err := range jsonstream.Decode[order.Order](res.Body, stream) {
			if !yield(o, err) {
				return
			}
		}
	}
}
