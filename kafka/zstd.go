package kafka

import (
	"sync"

	"github.com/klauspost/compress/zstd"
)

var (
	zstdDec *zstd.Decoder

	zstdDecOnce sync.Once
)

func zstdDecompress(dst, src []byte) ([]byte, error) {
	zstdDecOnce.Do(func() {
		zstdDec, _ = zstd.NewReader(nil)
	})
	return zstdDec.DecodeAll(src, dst)
}
