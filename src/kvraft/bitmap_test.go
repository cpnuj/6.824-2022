package kvraft

import "testing"

func TestBitmap(t *testing.T) {
	tests := []int{1, 11, 4, 38, 42, 83180, 6, 6, 4, 77, 0, 2, 3, 5, 7, 8, 1411}
	bitmap := NewBitmap()
	for _, i := range tests {
		bitmap.Set(i)
	}
	for _, i := range tests {
		if bitmap.IsSet(i) == false {
			t.Errorf("%d should be set, but not.", i)
		}
	}
	bitmap.Shrink()
	tests2 := []int{3910, 31, 8109830, 313, 318, 99, 9310, 88}
	for _, i := range tests {
		if bitmap.IsSet(i) == false {
			t.Errorf("%d should be set, but not.", i)
		}
	}
	for _, i := range tests2 {
		if bitmap.IsSet(i) == true {
			t.Errorf("%d should not be set, but got it.", i)
		}
	}
	for _, i := range tests2 {
		bitmap.Set(i)
	}
	for _, i := range tests {
		if bitmap.IsSet(i) == false {
			t.Errorf("%d should be set, but not.", i)
		}
	}
	for _, i := range tests2 {
		if bitmap.IsSet(i) == false {
			t.Errorf("%d should be set, but not.", i)
		}
	}
}
