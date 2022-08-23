package kvraft

import (
	"math"
)

type Bitmap struct {
	// invariant: Start % 8 == 0
	Start   int
	Content []uint8
}

func NewBitmap() *Bitmap {
	return &Bitmap{
		Start:   0,
		Content: []uint8{},
	}
}

func (b *Bitmap) Set(i int) {
	if i < b.Start {
		return
	}
	i = i - b.Start
	blk, off := i/8, i%8
	for i := blk - len(b.Content); i >= 0; i-- {
		b.Content = append(b.Content, 0)
	}
	b.Content[blk] |= 1 << off
}

func (b *Bitmap) IsSet(i int) bool {
	if i < b.Start {
		return true
	}
	i = i - b.Start
	blk, off := i/8, i%8
	if blk >= len(b.Content) {
		return false
	}
	return (b.Content[blk] & (1 << off)) > 0
}

const uint8Max = uint8(math.MaxUint8)

// Shrink will find continuous set bits from beginning, remove them and
// update the Start index.
func (b *Bitmap) Shrink() {
	toShrink := 0
	for i := range b.Content {
		if b.Content[i]&uint8Max != uint8Max {
			break
		}
		b.Start += 8
		toShrink++
	}
	if toShrink == len(b.Content) {
		b.Content = []uint8{}
	} else {
		b.Content = b.Content[toShrink:]
	}
	// fmt.Printf("after shrink %d\n", b.Start)
}
