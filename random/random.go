package random

import (
	"math"
	"math/rand"
	"strings"
	"time"
)

type StringGenerator struct {
	base       string
	baseLength int
	indexBits  int
	indexMask  int64
	indexMax   int
	source     rand.Source
}

func NewStringGenerator(base string) *StringGenerator {
	y := 1.0
	for math.Pow(2, y) < float64(len(base)) {
		y = y + 1
	}
	bits := int(y)

	source := rand.NewSource(time.Now().UnixNano())
	return &StringGenerator{
		base:       base,
		baseLength: len(base),
		indexBits:  bits,
		indexMask:  (1 << bits) - 1,
		indexMax:   63 / bits,
		source:     source,
	}
}

func (rsg *StringGenerator) Generate(length int) string {
	builder := strings.Builder{}
	builder.Grow(length)

	for i, cache, remain := length-1, rsg.source.Int63(), rsg.indexMax; i >= 0; {
		if remain == 0 {
			cache = rsg.source.Int63()
			remain = rsg.indexMax
		}

		if index := int(cache & rsg.indexMask); index < rsg.baseLength {
			builder.WriteByte(rsg.base[index])
			i--
		}

		cache >>= rsg.indexBits
		remain--
	}

	return builder.String()
}
