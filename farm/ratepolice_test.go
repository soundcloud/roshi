package farm

import (
	"testing"
	"time"
)

func TestNoPolice(t *testing.T) {
	rp := NewNoPolice()
	rp.Report(123456789)
	if expected, got := MaxInt, rp.Request(-1); expected != got {
		t.Errorf("Expected %v, got %v.", expected, got)
	}
}

func TestRatePolice(t *testing.T) {
	rp := NewRatePolice(100*time.Millisecond, 10)
	if expected, got := 20, rp.Request(1000); expected != got {
		t.Errorf("Expected %v, got %v.", expected, got)
	}
	rp.Report(90)
	if expected, got := 10, rp.Request(1000); expected != got {
		t.Errorf("Expected %v, got %v.", expected, got)
	}
	time.Sleep(15 * time.Millisecond)
	rp.Report(5)
	if expected, got := 5, rp.Request(1000); expected != got {
		t.Errorf("Expected %v, got %v.", expected, got)
	}
	time.Sleep(10 * time.Millisecond)
	rp.Report(3)
	if expected, got := 2, rp.Request(1000); expected != got {
		t.Errorf("Expected %v, got %v.", expected, got)
	}
	time.Sleep(70 * time.Millisecond)
	rp.Report(80)
	if expected, got := -78, rp.Request(1000); expected != got {
		t.Errorf("Expected %v, got %v.", expected, got)
	}
	time.Sleep(10 * time.Millisecond)
	if expected, got := 12, rp.Request(1000); expected != got {
		t.Errorf("Expected %v, got %v.", expected, got)
	}
	time.Sleep(10 * time.Millisecond)
	rp.Report(2)
	if expected, got := 15, rp.Request(1000); expected != got {
		t.Errorf("Expected %v, got %v.", expected, got)
	}
	time.Sleep(10 * time.Millisecond)
	if expected, got := 18, rp.Request(1000); expected != got {
		t.Errorf("Expected %v, got %v.", expected, got)
	}
	time.Sleep(100 * time.Millisecond)
	if expected, got := 20, rp.Request(1000); expected != got {
		t.Errorf("Expected %v, got %v.", expected, got)
	}
}
