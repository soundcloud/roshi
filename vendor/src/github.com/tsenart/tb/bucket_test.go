package tb

import (
	"testing"
	"time"
)

func TestNewBucket(t *testing.T) {
	t.Parallel()

	b := NewBucket(10, 0)
	b.Take(10)
	time.Sleep(100 * time.Millisecond)
	if w, g := int64(0), b.Take(1); w != g {
		t.Fatal("Expected no filling when freq < 1/c")
	}
}

func TestBucket_Take_single(t *testing.T) {
	t.Parallel()

	b := NewBucket(10, -1)
	defer b.Close()

	ex := [...]int64{5, 5, 1, 1, 5, 4, 1, 0}
	for i := 0; i < len(ex)-1; i += 2 {
		if got, want := b.Take(ex[i]), ex[i+1]; got != want {
			t.Errorf("Want: %d, Got: %d", want, got)
		}
	}
}

func TestBucket_Put_single(t *testing.T) {
	t.Parallel()

	b := NewBucket(10, 0)
	defer b.Close()

	b.Take(10)

	ex := [...]int64{5, 5, 10, 5, 15, 0}
	for i := 0; i < len(ex)-1; i += 2 {
		if got, want := b.Put(ex[i]), ex[i+1]; got != want {
			t.Errorf("Want: %d, Got: %d", want, got)
		}
	}
}

func TestBucket_Take_multi(t *testing.T) {
	t.Parallel()

	b := NewBucket(10, -1)
	defer b.Close()

	exs := [2][]int64{{4, 4, 2, 2}, {2, 2, 1, 1}}
	for i := 0; i < 2; i++ {
		go func(i int) {
			for j := 0; j < len(exs[i])-1; j += 2 {
				if got, want := b.Take(exs[i][j]), exs[i][j+1]; got != want {
					t.Errorf("Want: %d, Got: %d", want, got)
				}
			}
		}(i)
	}
}

func TestBucket_Put_multi(t *testing.T) {
	t.Parallel()

	b := NewBucket(10, 0)
	defer b.Close()

	b.Take(10)

	exs := [2][]int64{{4, 4, 2, 2}, {2, 2, 1, 1}}
	for i := 0; i < 2; i++ {
		go func(i int) {
			for j := 0; j < len(exs[i])-1; j += 2 {
				if got, want := b.Put(exs[i][j]), exs[i][j+1]; got != want {
					t.Errorf("Want: %d, Got: %d", want, got)
				}
			}
		}(i)
	}
}

func TestBucket_Take_throughput(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("Skipping test in short mode")
	}

	b := NewBucket(1000, -1)
	defer b.Close()

	b.Take(1000)

	var (
		out   int64
		began = time.Now()
	)
	for out < 1000 {
		out += b.Take(1000 - out)
	}

	ended := time.Since(began)
	if int(ended.Seconds()) != 1 {
		t.Errorf("Want 1000 tokens to take 1s. Got: %d", int(ended.Seconds()))
	}
}

func BenchmarkBucket_Take_sequential(b *testing.B) {
	bucket := NewBucket(int64(b.N), -1)
	defer bucket.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bucket.Take(8)
	}
}

func BenchmarkBucket_Put_sequential(b *testing.B) {
	bucket := NewBucket(int64(b.N), 0)
	defer bucket.Close()

	bucket.Take(int64(b.N))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bucket.Put(8)
	}
}
func TestBucket_Close(t *testing.T) {
	b := NewBucket(10000, -1)
	b.Close()
	b.Take(10000)
	time.Sleep(10 * time.Millisecond)
	if want, got := int64(0), b.Take(1); want != got {
		t.Errorf("Want: %d Got: %d", want, got)
	}
}
