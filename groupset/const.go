package groupset

type GroupKey uint8

const (
	G_sex GroupKey = iota
	G_status
	G_interests
	G_country
	G_city

	glength
)

const (
	F_query_id uint = iota
	F_limit
	F_order
	F_keys

	F_sex
	F_email
	F_status
	F_fname
	F_sname
	F_phone
	F_country
	F_city
	F_birth
	F_joined
	F_interests
	F_likes
	F_premium

	Flength
)

var mapQueryKeys = map[string]GroupKey{
	"sex":       G_sex,
	"status":    G_status,
	"interests": G_interests,
	"country":   G_country,
	"city":      G_city,
}

var mapQueryFilter = map[string]uint{
	"query_id": F_query_id,
	"limit":    F_limit,
	"order":    F_order,
	"keys":     F_keys,

	"sex":       F_sex,
	"email":     F_email,
	"status":    F_status,
	"fname":     F_fname,
	"sname":     F_sname,
	"phone":     F_phone,
	"country":   F_country,
	"city":      F_city,
	"birth":     F_birth,
	"joined":    F_joined,
	"interests": F_interests,
	"likes":     F_likes,
}
