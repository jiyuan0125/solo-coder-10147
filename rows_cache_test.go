package pgx

import "sync"

func StructRowFieldCacheLen() int {
	n := 0
	structRowFieldCache.Range(func(_, _ any) bool {
		n++
		return true
	})
	return n
}

func StructRowFieldCacheClear() {
	structRowFieldCache = sync.Map{}
}
