package userresponse

import "testing"

func TestDecode(t *testing.T) {
	cases := []struct {
		raw       string
		inputType string
		want      string
		skipped   bool
	}{
		{raw: "__skipped__", skipped: true},
		{raw: "__skip__", inputType: "multiselect", skipped: true},
		{raw: "__custom__:Rust", want: "Rust"},
		{raw: "__custom__:Rust", inputType: "select", want: "Rust"},
		{raw: "approve", inputType: "approval", want: "approve"},
		{raw: `["ai","__custom__:Rust"]`, inputType: "multiselect", want: `["ai","Rust"]`},
		{raw: `["__custom__:only typed text"]`, inputType: "multiselect", want: `["only typed text"]`},
		{raw: `["web", "__custom__:R&D <café>, \"quoted\""]`, inputType: "multiselect", want: `["web","R&D <café>, \"quoted\""]`},
		{raw: `["ai",2,null,"__custom__:Go"]`, inputType: "multiselect", want: `["ai",2,null,"Go"]`},
		{raw: `["ai","__custom__:Rust"]`, inputType: "choice", want: `["ai","Rust"]`},
		{raw: `["ai","__custom__:Rust"]`, inputType: "pick_many", want: `["ai","Rust"]`},
		{raw: `["ai","web"]`, inputType: "multiselect", want: `["ai","web"]`},
		{raw: `["ai", "web"]`, inputType: "multiselect", want: `["ai", "web"]`},
		{raw: `["ai","mentions __custom__: inside"]`, inputType: "multiselect", want: `["ai","mentions __custom__: inside"]`},
		{raw: `[not json __custom__:x`, inputType: "multiselect", want: `[not json __custom__:x`},
		// A JSON array answering any other question is text the person typed.
		{raw: `["ai","__custom__:Rust"]`, inputType: "text", want: `["ai","__custom__:Rust"]`},
		{raw: `["ai","__custom__:Rust"]`, inputType: "select", want: `["ai","__custom__:Rust"]`},
		{raw: `["ai","__custom__:Rust"]`, inputType: "approval", want: `["ai","__custom__:Rust"]`},
		{raw: `["ai","__custom__:Rust"]`, inputType: "", want: `["ai","__custom__:Rust"]`},
	}
	for _, tc := range cases {
		got, skipped := Decode(tc.raw, tc.inputType)
		if got != tc.want || skipped != tc.skipped {
			t.Errorf("Decode(%q, %q) = %q, %v; want %q, %v", tc.raw, tc.inputType, got, skipped, tc.want, tc.skipped)
		}
	}
}
