package groupset

import (
	"github.com/covrom/highloadcup2018/db"
	"sort"
)

type GroupStats struct {
	gs    *GroupSet
	stats []GroupResult
}

func NewGroupStats(gs *GroupSet) *GroupStats {
	return &GroupStats{
		gs:    gs,
		stats: make([]GroupResult, 0, 60),
	}
}

func (stg *GroupStats) Result() []GroupResult {
	lim := len(stg.stats)
	if lim > int(stg.gs.Limit) {
		lim = int(stg.gs.Limit)
	}
	return stg.stats[:lim]
}

func (stg *GroupStats) Update(fwd, rev func(int) bool, gr GroupResult) {
	var idx int
	if stg.gs.Order > 0 {
		idx = sort.Search(len(stg.stats), fwd)
	} else {
		idx = sort.Search(len(stg.stats), rev)
	}
	if idx < len(stg.stats) {
		if len(stg.stats) < 60 {
			stg.stats = append(stg.stats, GroupResult{})
		}
		copy(stg.stats[idx+1:], stg.stats[idx:])
		stg.stats[idx] = gr
	} else if len(stg.stats) < 60 {
		stg.stats = append(stg.stats, gr)
	}
}

func (stg *GroupStats) fwd_sex_country(sex byte, country db.DataEntry, v uint32) func(int) bool {
	return func(ii int) bool {
		if stg.stats[ii].Count > v {
			return true
		} else if stg.stats[ii].Count == v {
			if stg.gs.KeysOrder[G_sex] < stg.gs.KeysOrder[G_country] {
				if stg.stats[ii].Sex > sex {
					return true
				} else if stg.stats[ii].Sex == sex {
					return db.SmAcc.Country.DictonaryCompare(stg.stats[ii].Country, country) >= 0
				}
			} else {
				cmp := db.SmAcc.Country.DictonaryCompare(stg.stats[ii].Country, country)
				if cmp > 0 {
					return true
				} else if cmp == 0 {
					return stg.stats[ii].Sex >= sex
				}
			}
		}
		return false
	}
}

func (stg *GroupStats) rev_sex_country(sex byte, country db.DataEntry, v uint32) func(int) bool {
	return func(ii int) bool {
		if stg.stats[ii].Count < v {
			return true
		} else if stg.stats[ii].Count == v {
			if stg.gs.KeysOrder[G_sex] < stg.gs.KeysOrder[G_country] {
				if stg.stats[ii].Sex < sex {
					return true
				} else if stg.stats[ii].Sex == sex {
					return db.SmAcc.Country.DictonaryCompare(stg.stats[ii].Country, country) <= 0
				}
			} else {
				cmp := db.SmAcc.Country.DictonaryCompare(stg.stats[ii].Country, country)
				if cmp < 0 {
					return true
				} else if cmp == 0 {
					return stg.stats[ii].Sex <= sex
				}
			}
		}
		return false
	}
}

func (stg *GroupStats) fwd_sex_city(sex byte, city db.DataEntry, v uint32) func(int) bool {
	return func(ii int) bool {
		if stg.stats[ii].Count > v {
			return true
		} else if stg.stats[ii].Count == v {
			if stg.gs.KeysOrder[G_sex] < stg.gs.KeysOrder[G_city] {
				if stg.stats[ii].Sex > sex {
					return true
				} else if stg.stats[ii].Sex == sex {
					return db.SmAcc.City.DictonaryCompare(stg.stats[ii].City, city) >= 0
				}
			} else {
				cmp := db.SmAcc.City.DictonaryCompare(stg.stats[ii].City, city)
				if cmp > 0 {
					return true
				} else if cmp == 0 {
					return stg.stats[ii].Sex >= sex
				}
			}
		}
		return false
	}
}

func (stg *GroupStats) rev_sex_city(sex byte, city db.DataEntry, v uint32) func(int) bool {
	return func(ii int) bool {
		if stg.stats[ii].Count < v {
			return true
		} else if stg.stats[ii].Count == v {
			if stg.gs.KeysOrder[G_sex] < stg.gs.KeysOrder[G_city] {
				if stg.stats[ii].Sex < sex {
					return true
				} else if stg.stats[ii].Sex == sex {
					return db.SmAcc.City.DictonaryCompare(stg.stats[ii].City, city) <= 0
				}
			} else {
				cmp := db.SmAcc.City.DictonaryCompare(stg.stats[ii].City, city)
				if cmp < 0 {
					return true
				} else if cmp == 0 {
					return stg.stats[ii].Sex <= sex
				}
			}
		}
		return false
	}
}

func (stg *GroupStats) fwd_status_city(status, city db.DataEntry, v uint32) func(int) bool {
	return func(ii int) bool {
		if stg.stats[ii].Count > v {
			return true
		} else if stg.stats[ii].Count == v {
			if stg.gs.KeysOrder[G_status] < stg.gs.KeysOrder[G_city] {
				cmp := db.SmAcc.Status.DictonaryCompare(stg.stats[ii].Status, status)
				if cmp > 0 {
					return true
				} else if cmp == 0 {
					return db.SmAcc.City.DictonaryCompare(stg.stats[ii].City, city) >= 0
				}
			} else {
				cmp := db.SmAcc.City.DictonaryCompare(stg.stats[ii].City, city)
				if cmp > 0 {
					return true
				} else if cmp == 0 {
					return db.SmAcc.Status.DictonaryCompare(stg.stats[ii].Status, status) >= 0
				}
			}
		}
		return false
	}
}

func (stg *GroupStats) rev_status_city(status, city db.DataEntry, v uint32) func(int) bool {
	return func(ii int) bool {
		if stg.stats[ii].Count < v {
			return true
		} else if stg.stats[ii].Count == v {
			if stg.gs.KeysOrder[G_status] < stg.gs.KeysOrder[G_city] {
				cmp := db.SmAcc.Status.DictonaryCompare(stg.stats[ii].Status, status)
				if cmp < 0 {
					return true
				} else if cmp == 0 {
					return db.SmAcc.City.DictonaryCompare(stg.stats[ii].City, city) <= 0
				}
			} else {
				cmp := db.SmAcc.City.DictonaryCompare(stg.stats[ii].City, city)
				if cmp < 0 {
					return true
				} else if cmp == 0 {
					return db.SmAcc.Status.DictonaryCompare(stg.stats[ii].Status, status) <= 0
				}
			}
		}
		return false
	}
}

func (stg *GroupStats) fwd_status_country(status, country db.DataEntry, v uint32) func(int) bool {
	return func(ii int) bool {
		if stg.stats[ii].Count > v {
			return true
		} else if stg.stats[ii].Count == v {
			if stg.gs.KeysOrder[G_status] < stg.gs.KeysOrder[G_country] {
				cmp := db.SmAcc.Status.DictonaryCompare(stg.stats[ii].Status, status)
				if cmp > 0 {
					return true
				} else if cmp == 0 {
					return db.SmAcc.Country.DictonaryCompare(stg.stats[ii].Country, country) >= 0
				}
			} else {
				cmp := db.SmAcc.Country.DictonaryCompare(stg.stats[ii].Country, country)
				if cmp > 0 {
					return true
				} else if cmp == 0 {
					return db.SmAcc.Status.DictonaryCompare(stg.stats[ii].Status, status) >= 0
				}
			}
		}
		return false
	}
}

func (stg *GroupStats) rev_status_country(status, country db.DataEntry, v uint32) func(int) bool {
	return func(ii int) bool {
		if stg.stats[ii].Count < v {
			return true
		} else if stg.stats[ii].Count == v {
			if stg.gs.KeysOrder[G_status] < stg.gs.KeysOrder[G_country] {
				cmp := db.SmAcc.Status.DictonaryCompare(stg.stats[ii].Status, status)
				if cmp < 0 {
					return true
				} else if cmp == 0 {
					return db.SmAcc.Country.DictonaryCompare(stg.stats[ii].Country, country) <= 0
				}
			} else {
				cmp := db.SmAcc.Country.DictonaryCompare(stg.stats[ii].Country, country)
				if cmp < 0 {
					return true
				} else if cmp == 0 {
					return db.SmAcc.Status.DictonaryCompare(stg.stats[ii].Status, status) <= 0
				}
			}
		}
		return false
	}
}

func (stg *GroupStats) fwd_sex(sex byte, v uint32) func(int) bool {
	return func(ii int) bool {
		return stg.stats[ii].Count > v || (stg.stats[ii].Count == v && stg.stats[ii].Sex >= sex)
	}
}

func (stg *GroupStats) rev_sex(sex byte, v uint32) func(int) bool {
	return func(ii int) bool {
		return stg.stats[ii].Count < v || (stg.stats[ii].Count == v && stg.stats[ii].Sex <= sex)
	}
}

func (stg *GroupStats) fwd_country(country db.DataEntry, v uint32) func(int) bool {
	return func(ii int) bool {
		return stg.stats[ii].Count > v || (stg.stats[ii].Count == v &&
			db.SmAcc.Country.DictonaryCompare(stg.stats[ii].Country, country) >= 0)
	}
}

func (stg *GroupStats) rev_country(country db.DataEntry, v uint32) func(int) bool {
	return func(ii int) bool {
		return stg.stats[ii].Count < v || (stg.stats[ii].Count == v &&
			db.SmAcc.Country.DictonaryCompare(stg.stats[ii].Country, country) <= 0)
	}
}

func (stg *GroupStats) fwd_city(city db.DataEntry, v uint32) func(int) bool {
	return func(ii int) bool {
		return stg.stats[ii].Count > v || (stg.stats[ii].Count == v &&
			db.SmAcc.City.DictonaryCompare(stg.stats[ii].City, city) >= 0)
	}
}

func (stg *GroupStats) rev_city(city db.DataEntry, v uint32) func(int) bool {
	return func(ii int) bool {
		return stg.stats[ii].Count < v || (stg.stats[ii].Count == v &&
			db.SmAcc.City.DictonaryCompare(stg.stats[ii].City, city) <= 0)
	}
}

func (stg *GroupStats) fwd_status(status db.DataEntry, v uint32) func(int) bool {
	return func(ii int) bool {
		return stg.stats[ii].Count > v || (stg.stats[ii].Count == v &&
			db.SmAcc.Status.DictonaryCompare(stg.stats[ii].Status, status) >= 0)
	}
}

func (stg *GroupStats) rev_status(status db.DataEntry, v uint32) func(int) bool {
	return func(ii int) bool {
		return stg.stats[ii].Count < v || (stg.stats[ii].Count == v &&
			db.SmAcc.Status.DictonaryCompare(stg.stats[ii].Status, status) <= 0)
	}
}

func (stg *GroupStats) fwd_interest(interest db.DataEntry, v uint32) func(int) bool {
	return func(ii int) bool {
		return stg.stats[ii].Count > v || (stg.stats[ii].Count == v &&
			db.SmAcc.Interests.DictonaryCompare(stg.stats[ii].Interests, interest) >= 0)
	}
}

func (stg *GroupStats) rev_interest(interest db.DataEntry, v uint32) func(int) bool {
	return func(ii int) bool {
		return stg.stats[ii].Count < v || (stg.stats[ii].Count == v &&
			db.SmAcc.Interests.DictonaryCompare(stg.stats[ii].Interests, interest) <= 0)
	}
}
