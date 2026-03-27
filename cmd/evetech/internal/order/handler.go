package order

import (
	"cmp"
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"iter"
	"net/url"
	"runtime/trace"

	"golang.org/x/sync/errgroup"
)

const defaultBufSize = 1
const defaultLimit = 1 << 0

type Client interface {
	Regions(ctx context.Context, u *url.URL) iter.Seq2[uint64, error]
	Max(ctx context.Context, u *url.URL, region uint64) (uint64, error)
	Orders(ctx context.Context, u *url.URL, region, page uint64) iter.Seq2[Order, error]
}

type Handler struct{ Client }

func (h *Handler) OrdersReader(ctx context.Context, u *url.URL, hasHeader bool) io.ReadCloser {
	g, ctx := errgroup.WithContext(ctx)

	regions := make(chan uint64, defaultBufSize)
	g.Go(func() error {
		ctx, task := trace.NewTask(ctx, "regions")
		defer func() { close(regions); task.End() }()

		for id, err := range h.Client.Regions(ctx, u) {
			if err != nil {
				return err
			}
			wait := trace.StartRegion(ctx, "wait")
			select {
			case <-ctx.Done():
				wait.End()
				return ctx.Err()
			case regions <- id:
				wait.End()
			}
		}
		return nil
	})

	type query struct{ region, page uint64 }
	queries := make(chan query, defaultBufSize)
	g.Go(func() error {
		defer func() { close(queries) }()

		g, ctx := errgroup.WithContext(ctx)
		g.SetLimit(defaultLimit)
		for region := range regions {
			g.Go(func() error {
				ctx, task := trace.NewTask(ctx, "pages")
				defer func() { task.End() }()

				n, err := h.Client.Max(ctx, u, region)
				if err != nil {
					return fmt.Errorf("failed to fetch max page: %v", err)
				} // else if n < 1

				for i := range n {
					wait := trace.StartRegion(ctx, "wait")
					select {
					case <-ctx.Done():
						wait.End()
						return ctx.Err()
					case queries <- query{region, i + 1}:
						wait.End()
					}
				}
				return nil
			})
		}
		return g.Wait()
	})

	records := make(chan [12]string, defaultBufSize)
	g.Go(func() error {
		defer func() { close(records) }()

		// send the header if hasHeader is set
		if hasHeader {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case records <- [12]string{
				"duration",
				"is_buy_order",
				"issued",
				"location_id",
				"min_volume",
				"order_id",
				"price",
				"range",
				"system_id",
				"type_id",
				"volume_remain",
				"volume_total",
			}:
			}
		}

		g, ctx := errgroup.WithContext(ctx)
		g.SetLimit(defaultLimit)
		for q := range queries {
			g.Go(func() error {
				ctx, task := trace.NewTask(ctx, "orders")
				defer func() { task.End() }()

				for o, err := range h.Client.Orders(ctx, u, q.region, q.page) {
					if err != nil {
						return err
					}
					wait := trace.StartRegion(ctx, "wait")
					select {
					case <-ctx.Done():
						wait.End()
						return ctx.Err()
					case records <- o.Record():
						wait.End()
					}
				}
				return nil
			})
		}
		return g.Wait()
	})

	pr, pw := io.Pipe()
	g.Go(func() error {
		cw := csv.NewWriter(pw) // 4*1<<10 buffer
		for record := range records {
			if err := cmp.Or(cw.Write(record[:]), ctx.Err()); err != nil {
				return err
			}
		}
		cw.Flush()
		return cw.Error()
	})
	go func() { pw.CloseWithError(g.Wait()) }()
	return pr
}
