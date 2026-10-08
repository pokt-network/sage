package qos

import (
	"testing"
	"time"
)

func TestHostMemory_ExpiresAndBounds(t *testing.T) {
	now := time.Unix(1_000_000, 0)
	m := NewHostMemory[uint64](time.Hour, 2)
	m.now = func() time.Time { return now }

	m.Set("a", 100)
	if got, ok := m.Get("a"); !ok || got != 100 {
		t.Fatalf("Get(a) = %d,%v want 100,true", got, ok)
	}
	now = now.Add(time.Hour + time.Second)
	if _, ok := m.Get("a"); ok {
		t.Fatal("an hour-old observation must have aged out")
	}

	m.Set("a", 100)
	m.Set("b", 200)
	m.Set("b", 250) // a known host never trips the cap
	if got, ok := m.Get("a"); !ok || got != 100 {
		t.Fatalf("Get(a) = %d,%v want 100,true: rewriting a known host must not clear", got, ok)
	}
	m.Set("c", 300) // past the cap: wholesale clear, then c alone
	if _, ok := m.Get("a"); ok {
		t.Fatal("cap exceeded should clear the memory")
	}
	if got, ok := m.Get("c"); !ok || got != 300 {
		t.Fatalf("Get(c) = %d,%v want 300,true", got, ok)
	}
	m.Set("", 5)
	if _, ok := m.Get(""); ok {
		t.Fatal("empty host must not be stored")
	}

	m.Reset()
	if _, ok := m.Get("c"); ok {
		t.Fatal("Reset must forget every host")
	}
}
