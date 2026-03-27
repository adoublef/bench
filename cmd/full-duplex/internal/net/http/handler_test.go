package http_test

import (
	"cmp"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	. "github.com/adoublef/bench/cmd/full-duplex/internal/net/http"
)

func TestHandler(t *testing.T) {
	t.Run("Map", func(t *testing.T) {
		ctx := t.Context()

		c, sURL := testClient(t)

		// need a reader
		body := strings.NewReader(strings.Repeat("Hello, world\n", 20<<20)) //4kb
		req, err1 := http.NewRequestWithContext(ctx, http.MethodPost, sURL+"/map", body)
		res, err2 := c.Do(req)
		ok(t, cmp.Or(err1, err2))
		n, err := io.Copy(io.Discard, res.Body)
		ok(t, err)
		t.Logf("%d, _ = io.Copy", n)
		ok(t, res.Body.Close())
	})
}

func testClient(t testing.TB) (*http.Client, string) {
	t.Helper()

	s := httptest.NewServer(Handler())
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
