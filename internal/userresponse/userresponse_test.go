package userresponse

import "testing"

func TestDecode(t *testing.T) {
	cases := []struct {
		raw     string
		want    string
		skipped bool
	}{
		{raw: "__skipped__", skipped: true},
		{raw: "__skip__", skipped: true},
		{raw: "__custom__:Rust", want: "Rust"},
		{raw: "approve", want: "approve"},
		{raw: `["ai","__custom__:Rust"]`, want: `["ai","Rust"]`},
		{raw: `["__custom__:only typed text"]`, want: `["only typed text"]`},
		{raw: `["web", "__custom__:R&D <café>, \"quoted\""]`, want: `["web","R&D <café>, \"quoted\""]`},
		{raw: `["ai",2,null,"__custom__:Go"]`, want: `["ai",2,null,"Go"]`},
		{raw: `["ai","web"]`, want: `["ai","web"]`},
		{raw: `["ai", "web"]`, want: `["ai", "web"]`},
		{raw: `["ai","mentions __custom__: inside"]`, want: `["ai","mentions __custom__: inside"]`},
		{raw: `[not json __custom__:x`, want: `[not json __custom__:x`},
	}
	for _, tc := range cases {
		got, skipped := Decode(tc.raw)
		if got != tc.want || skipped != tc.skipped {
			t.Errorf("Decode(%q) = %q, %v; want %q, %v", tc.raw, got, skipped, tc.want, tc.skipped)
		}
	}
}
