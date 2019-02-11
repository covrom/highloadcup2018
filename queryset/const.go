package queryset

const (
	F_query_id uint = iota
	F_limit
	F_sex_eq
	F_email_domain
	F_email_lt
	F_email_gt
	F_status_eq
	F_status_neq
	F_fname_eq
	F_fname_any
	F_fname_null
	F_sname_eq
	F_sname_starts
	F_sname_null
	F_phone_code
	F_phone_null
	F_country_eq
	F_country_null
	F_city_eq
	F_city_any
	F_city_null
	F_birth_year
	F_birth_lt
	F_birth_gt
	F_interests_contains
	F_interests_any
	F_likes_contains
	F_premium_now
	F_premium_null

	F_country
	F_city

	Flength
)

var mapQueryKeysFilter = map[string]uint{
	"query_id":           F_query_id,
	"limit":              F_limit,
	"sex_eq":             F_sex_eq,
	"email_domain":       F_email_domain,
	"email_lt":           F_email_lt,
	"email_gt":           F_email_gt,
	"status_eq":          F_status_eq,
	"status_neq":         F_status_neq,
	"fname_eq":           F_fname_eq,
	"fname_any":          F_fname_any,
	"fname_null":         F_fname_null,
	"sname_eq":           F_sname_eq,
	"sname_starts":       F_sname_starts,
	"sname_null":         F_sname_null,
	"phone_code":         F_phone_code,
	"phone_null":         F_phone_null,
	"country_eq":         F_country_eq,
	"country_null":       F_country_null,
	"city_eq":            F_city_eq,
	"city_any":           F_city_any,
	"city_null":          F_city_null,
	"birth_year":         F_birth_year,
	"birth_lt":           F_birth_lt,
	"birth_gt":           F_birth_gt,
	"interests_contains": F_interests_contains,
	"interests_any":      F_interests_any,
	"likes_contains":     F_likes_contains,
	"premium_now":        F_premium_now,
	"premium_null":       F_premium_null,
}

var mapQueryKeysRecommend = map[string]uint{
	"query_id": F_query_id,
	"limit":    F_limit,
	"country":  F_country,
	"city":     F_city,
}
