package ssdb

import (
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestLock(t *testing.T) {
	mu := NewKeyMutex()
	key := "key"

	mu.Lock(key)
	mu.Unlock(key)
}

func TestUnlock(t *testing.T) {
	mu := NewKeyMutex()
	key := "key"

	mu.Unlock(key)

	mu.Unlock(key)
}

func BenchmarkMutex(b *testing.B) {
	var mu sync.Mutex

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mu.Lock()
		time.Sleep(1 * time.Microsecond)
		mu.Unlock()
	}
}

func BenchmarkMutexParallel(b *testing.B) {
	var mu sync.Mutex

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			mu.Lock()
			time.Sleep(1 * time.Microsecond)
			mu.Unlock()
		}
	})
}

func BenchmarkKeyMutex(b *testing.B) {
	mu := NewKeyMutex()
	key := "key"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mu.Lock(key)
		time.Sleep(1 * time.Microsecond)
		mu.Unlock(key)
	}
}

func benchmarkKeyMutexParallel(b *testing.B, n int) {
	mu := NewKeyMutex()

	keys := make([]string, 0, n)
	for i := 0; i < n; i++ {
		keys = append(keys, "key"+strconv.Itoa(i))
	}

	var i int64

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			key := keys[atomic.AddInt64(&i, 1)%int64(n)]
			mu.Lock(key)
			time.Sleep(1 * time.Microsecond)
			mu.Unlock(key)
		}
	})
}

func BenchmarkKeyMutexParallel1(b *testing.B)    { benchmarkKeyMutexParallel(b, 1) }
func BenchmarkKeyMutexParallel2(b *testing.B)    { benchmarkKeyMutexParallel(b, 2) }
func BenchmarkKeyMutexParallel4(b *testing.B)    { benchmarkKeyMutexParallel(b, 4) }
func BenchmarkKeyMutexParallel8(b *testing.B)    { benchmarkKeyMutexParallel(b, 8) }
func BenchmarkKeyMutexParallel16(b *testing.B)   { benchmarkKeyMutexParallel(b, 16) }
func BenchmarkKeyMutexParallel32(b *testing.B)   { benchmarkKeyMutexParallel(b, 32) }
func BenchmarkKeyMutexParallel64(b *testing.B)   { benchmarkKeyMutexParallel(b, 64) }
func BenchmarkKeyMutexParallel128(b *testing.B)  { benchmarkKeyMutexParallel(b, 128) }
func BenchmarkKeyMutexParallel256(b *testing.B)  { benchmarkKeyMutexParallel(b, 256) }
func BenchmarkKeyMutexParallel512(b *testing.B)  { benchmarkKeyMutexParallel(b, 512) }
func BenchmarkKeyMutexParallel1024(b *testing.B) { benchmarkKeyMutexParallel(b, 1024) }
func BenchmarkKeyMutexParallel2048(b *testing.B) { benchmarkKeyMutexParallel(b, 2048) }
func BenchmarkKeyMutexParallel4096(b *testing.B) { benchmarkKeyMutexParallel(b, 4096) }
