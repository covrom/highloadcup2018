package queryset

import "github.com/covrom/highloadcup2018/db"

func FilterLimit(qs *QuerySet) {
	iter := db.SmAcc.Iterator()
	limit := qs.Limit
	for iter.HasNext() {
		if limit <= 0 || qs.HasBadVals {
			break
		}
		id := iter.NextID()
		qs.SortedResult = append(qs.SortedResult, id)
		limit--
	}
}
