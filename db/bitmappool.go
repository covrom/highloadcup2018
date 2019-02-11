package db

import (
	"sync"

	"github.com/RoaringBitmap/roaring"
)

var bitPool = sync.Pool{}

func GetBitmap() *roaring.Bitmap {
	sl := bitPool.Get()
	if sl != nil {
		vsl := sl.(*roaring.Bitmap)
		return vsl
	}
	return roaring.New()
}

func PutBitmap(sl *roaring.Bitmap) {
	if sl == nil {
		return
	}
	sl.Clear()
	bitPool.Put(sl)
}
