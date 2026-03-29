package html

import (
	"io"
	"iter"
	"net/url"

	"golang.org/x/net/html"
)

// Anchors iterates a given reader for valid urls.
func Anchors(r io.Reader) iter.Seq2[*url.URL, error] {
	return walk(r, "a", "href")
}

// Images iterates a given reader for valid urls.
func Images(r io.Reader) iter.Seq2[*url.URL, error] {
	return walk(r, "img", "src")
}

func walk(r io.Reader, tag, attr string) iter.Seq2[*url.URL, error] {
	// https://drstearns.github.io/tutorials/tokenizing/
	z := html.NewTokenizer(r)
	return func(yield func(*url.URL, error) bool) {
	LOOP:
		for {
			switch tt := z.Next(); tt {
			case html.ErrorToken:
				err := z.Err()
				if err != nil {
					if err == io.EOF {
						return
					}
					yield(nil, err)
					return
				}
			case html.StartTagToken, html.SelfClosingTagToken: // <img/>
				tn, more := z.TagName()
				if string(tn) == tag && more {
					var key, val []byte
					for more {
						key, val, more = z.TagAttr()
						if string(key) == attr && len(val) > 0 {
							if !yield(url.Parse(string(val))) {
								return
							}
							continue LOOP
						}
					}
				}
			}
		}
	}
}
