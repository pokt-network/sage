package heuristic

import "testing"

func TestIsEmpty(t *testing.T) {
	tests := []struct {
		name string
		body []byte
		want bool
	}{
		{"nil", nil, true},
		{"empty", []byte(""), true},
		{"whitespace", []byte("  \n\t "), true},
		{"non-empty", []byte("hello"), false},
		{"json", []byte(`{"a":1}`), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsEmpty(tt.body); got != tt.want {
				t.Errorf("IsEmpty() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestIsHTML(t *testing.T) {
	tests := []struct {
		name string
		body []byte
		want bool
	}{
		{"DOCTYPE", []byte("<!DOCTYPE html><html><body>Error</body></html>"), true},
		{"doctype lower", []byte("<!doctype html><html></html>"), true},
		{"html tag", []byte("<html><body>502</body></html>"), true},
		{"HTML upper", []byte("<HTML><BODY>Error</BODY></HTML>"), true},
		{"not html - json", []byte(`{"error":"bad"}`), false},
		{"not html - plain", []byte("Service unavailable"), false},
		{"empty", []byte(""), false},
		{"xml", []byte(`<?xml version="1.0"?><error/>`), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsHTML(tt.body); got != tt.want {
				t.Errorf("IsHTML() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestIsPlainText(t *testing.T) {
	tests := []struct {
		name string
		body []byte
		want bool
	}{
		{"text", []byte("Service Temporarily Unavailable"), true},
		{"json", []byte(`{"error":"bad"}`), false},
		{"array json", []byte(`[1,2,3]`), false},
		{"html", []byte("<html>error</html>"), false},
		{"xml", []byte("<?xml version='1.0'?>"), false},
		{"empty", []byte(""), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsPlainText(tt.body); got != tt.want {
				t.Errorf("IsPlainText() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestIsXML(t *testing.T) {
	tests := []struct {
		name string
		body []byte
		want bool
	}{
		{"xml declaration", []byte(`<?xml version="1.0"?><root/>`), true},
		{"XML upper", []byte(`<?XML version="1.0"?><root/>`), true},
		{"xml error", []byte(`<Error><Code>500</Code></Error>`), true},
		{"html", []byte("<!DOCTYPE html><html></html>"), false},
		{"json", []byte(`{"a":1}`), false},
		{"empty", []byte(""), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsXML(tt.body); got != tt.want {
				t.Errorf("IsXML() = %v, want %v", got, tt.want)
			}
		})
	}
}
