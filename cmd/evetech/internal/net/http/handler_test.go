package http_test

import (
	"cmp"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"

	. "github.com/adoublef/bench/cmd/evetech/internal/net/http"
	"github.com/adoublef/bench/cmd/evetech/internal/order"
)

func TestHandler(t *testing.T) {
	t.Run("OK", func(t *testing.T) {
		ctx := t.Context()

		const numRegions = 1 << 0
		const numPages = 1 << 0
		const numOrders = 1 << 0

		apiC, apiURL := apiClient(t, numRegions, numPages, numOrders)
		c, sURL := testClient(t, apiC)

		url := fmt.Sprintf(`%s/?base_url=%s`, sURL, apiURL)
		req, err1 := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		res, err2 := c.Do(req)
		ok(t, cmp.Or(err1, err2))
		defer res.Body.Close()

		equal(t, res.StatusCode, http.StatusOK)
		equal(t, res.Header.Get("Content-Type"), "text/csv")
		// check content-disposition

		cr := csv.NewReader(res.Body)
		cr.ReuseRecord = true

		set := map[string]int{}
	LOOP:
		for {
			rr, err := cr.Read()
			if err == io.EOF {
				break LOOP
			}
			// check the size
			ok(t, err)
			equal(t, len(rr), 12)
			set[rr[5]]++ // orderId is unique
		}
		ok(t, res.Body.Close())

		// do we include the header?
		equal(t, len(set), numRegions)
		for _, n := range set {
			equal(t, n, 0+(numPages*numOrders)) // include the header
		}
	})
}

func testClient(t testing.TB, httpC *http.Client) (*http.Client, string) {
	t.Helper()

	st := &order.Handler{
		Client: &Client{httpC},
	}

	s := httptest.NewServer(Handler(st))
	t.Cleanup(func() { s.Close() })

	return s.Client(), s.URL
}

func ok(t testing.TB, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("%s: unexpected error: %v", t.Name(), err)
	}
}

func equal[K comparable](t testing.TB, got, want K) {
	t.Helper()
	if got != want {
		t.Fatalf("%s: got %v; want %v", t.Name(), got, want)
	}
}

func apiClient(t testing.TB, regions, max, orders int) (httpC *http.Client, baseURL string) {
	t.Helper()

	mux := http.NewServeMux()

	{ // GET /v1/universe/regions
		const start = 10000
		var rr = make([]int, regions)
		for i := range regions {
			rr[i] = start + (i + 1)
		}
		p, err := json.Marshal(rr)
		if err != nil {
			t.Fail()
		}

		mux.HandleFunc("GET /v1/universe/regions", func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Length", strconv.Itoa(len(p)))
			if _, err := w.Write(p); err != nil {
				t.Fail()
			}
		})
	}

	{ // HEAD /v1/markets/{id}/orders
		mux.HandleFunc("HEAD /v1/markets/{id}/orders", func(w http.ResponseWriter, r *http.Request) {
			_, err := strconv.ParseUint(r.PathValue("id"), 10, 64)
			if err != nil {
				t.Fail()
			}
			w.Header().Set("x-pages", strconv.Itoa(max))
		})
	}

	{ // GET /v1/markets/{id}/orders
		var oo = make([]order.Order, orders)
		for i := range orders {
			oo[i] = order.Order{
				OrderID:      i + 1,
				IsBuyOrder:   false, // or true
				Issued:       "issued",
				LocationID:   1,
				MinVolume:    1,
				Price:        1,
				Range:        "range",
				SystemID:     1,
				TypeID:       1,
				VolumeRemain: 1,
				VolumeTotal:  1,
			}
		}
		p, err := json.Marshal(oo)
		if err != nil {
			t.Fail()
		}

		mux.HandleFunc("GET /v1/markets/{id}/orders", func(w http.ResponseWriter, r *http.Request) {
			_, err1 := strconv.ParseUint(r.PathValue("id"), 10, 64)
			_, err2 := strconv.ParseUint(r.URL.Query().Get("page"), 10, 64)
			if err := cmp.Or(err1, err2); err != nil {
				t.Fail()
			}
			w.Header().Set("Content-Length", strconv.Itoa(len(p)))
			if _, err := w.Write(p); err != nil {
				t.Fail()
			}
		})
	}

	// See https://martin.baillie.id/wrote/gotchas-in-the-go-network-packages-defaults/
	s := httptest.NewServer(mux)
	return s.Client(), s.URL
}
