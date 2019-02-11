package account

import (
	"io"
	"regexp"
	"strings"

	"github.com/covrom/highloadcup2018/db"
	"github.com/covrom/highloadcup2018/dict"
)

var RePhoneCode *regexp.Regexp

type JSONMask uint16

const (
	_ JSONMask = 1 << iota
	M_fname
	M_sname
	M_phone
	M_sex
	M_birth
	M_country
	M_city
	M_joined
	M_status
	M_premium
	M_recosugg
)

const MaskRecommend = M_status | M_fname | M_sname | M_birth | M_premium | M_recosugg
const MaskSuggest = M_status | M_fname | M_sname | M_recosugg

type PremiumInterval struct {
	Start  db.TimeStamp `json:"start"`
	Finish db.TimeStamp `json:"finish"`
}

type OneLike struct {
	ID    uint32       `json:"id"`
	Stamp db.TimeStamp `json:"ts"`
}

type Account struct {
	// id - уникальный внешний идентификатор пользователя.
	// Устанавливается тестирующей системой и используется затем, для проверки ответов сервера.
	// Тип - 32-разрядное целое число.
	ID uint32 `json:"id"`

	// адрес электронной почты пользователя. Тип - unicode-строка длиной до 100 символов.
	// Гарантируется уникальность.
	Email string `json:"email"`

	// fname и sname - имя и фамилия соответственно. Тип - unicode-строки длиной до 50 символов.
	// Поля опциональны и могут отсутствовать в конкретной записи.
	FirstName  string `json:"fname,omitempty"`
	SecondName string `json:"sname,omitempty"`

	// phone - номер мобильного телефона. Тип - unicode-строка длиной до 16 символов.
	// Поле является опциональным, но для указанных значений гарантируется уникальность. Заполняется довольно редко.
	Phone string `json:"phone,omitempty"`

	// sex - unicode-строка "m" означает мужской пол, а "f" - женский.
	Sex string `json:"sex"`

	// birth - дата рождения, записанная как число секунд от начала UNIX-эпохи по UTC (другими словами - это timestamp).
	// Ограничено снизу 01.01.1950 и сверху 01.01.2005-ым.
	Birth db.TimeStamp `json:"birth"`

	// country - страна проживания. Тип - unicode-строка длиной до 50 символов. Поле опционально.
	Country string `json:"country,omitempty"`

	// city - город проживания. Тип - unicode-строка длиной до 50 символов.
	// Поле опционально и указывается редко. Каждый город расположен в определённой стране.
	City string `json:"city,omitempty"`

	// Также в одной записи Account есть поля специфичные для системы поиска "второй половинки":

	// joined - дата регистрации в системе. Тип - timestamp с ограничениями: снизу 01.01.2011, сверху 01.01.2018.
	Joined db.TimeStamp `json:"joined"`

	// status - текущий статус пользователя в системе.
	// Тип - одна строка из следующих вариантов: "свободны", "заняты", "всё сложно".
	// Не обращайте внимание на странные окончания :)
	Status string `json:"status"`

	// interests - интересы пользователя в обычной жизни.
	// Тип - массив unicode-строк, возможно пустой.
	// Строки не превышают по длине 100 символов.
	Interests []string `json:"interests"`

	// premium - начало и конец премиального периода в системе (когда пользователям очень хотелось найти "вторую половинку" и они делали денежный вклад).
	// В json это поле представлено вложенным объектом с полями start и finish, где записаны timestamp-ы с нижней границей 01.01.2018.
	Premium PremiumInterval `json:"premium"`

	// likes - массив известных симпатий пользователя, возможно пустой.
	// Все симпатии идут вразнобой и каждая представляет собой объект из следующих полей:
	// 	id - идентификатор другого аккаунта, к которому симпатия.
	// 		Аккаунт по id в исходных данных всегда существует.
	//		В данных может быть несколько лайков с одним и тем же id.
	// 	ts - время, то есть timestamp, когда симпатия была записана в систему.
	Likes []OneLike `json:"likes"`
}

type OneUpdateLike struct {
	Likee uint32       `json:"likee"`
	Liker uint32       `json:"liker"`
	Stamp db.TimeStamp `json:"ts"`
}

type UpdateLikes struct {
	Likes []OneUpdateLike `json:"likes"`
}

type Accounts struct {
	Accounts []Account `json:"accounts"`
}

func ConvertAccountToSmall(src Account, sas *db.SmallAccounts, async bool) (ret db.SmallAccount, likes []db.Like) {

	// все строковые данные собираем в словари в базу данных
	// а сами аккаунты храним в компактном виде в памяти, как int32

	ret.ID = db.IDAcc(src.ID)
	i := strings.IndexByte(src.Email, '@')
	if i >= 0 && i < (len(src.Email)-1) {
		sas.Domain.SetString(ret.ID, src.Email[i+1:], false, async)
	}
	dict.DictonaryEml.Put(src.ID, src.Email)
	sas.FirstName.SetString(ret.ID, src.FirstName, false, async)
	sas.SecondName.SetString(ret.ID, src.SecondName, false, async)

	ret.Phone = src.Phone
	phc := RePhoneCode.FindString(src.Phone)
	if len(phc) > 2 {
		sas.PhoneCode.SetString(ret.ID, phc[1:len(phc)-1], false, async)
	} else {
		sas.PhoneCode.SetString(ret.ID, "", false, async)
	}

	ret.Birth = src.Birth.Int()
	sas.BirthYear.Set(ret.ID, db.DataEntry(uint16(src.Birth.Time().Year())), false, async)

	status := sas.Status.ToDictonary(src.Status)
	sas.Status.Set(ret.ID, status, false, async)

	country := sas.Country.ToDictonary(src.Country)
	sas.Country.Set(ret.ID, country, false, async)

	city := sas.City.ToDictonary(src.City)
	sas.City.Set(ret.ID, city, false, async)

	sex := db.DataEntry(0)
	if src.Sex == "m" {
		sex = db.DataEntry(1)
	}
	sas.Sex.Set(ret.ID, sex, false, async)
	ret.Sex = src.Sex[0]

	sas.StatusCity.Set(ret.ID, city<<2|status, false, async)
	sas.StatusCountry.Set(ret.ID, country<<2|status, false, async)
	sas.SexCity.Set(ret.ID, city<<1|sex, false, async)
	sas.SexCountry.Set(ret.ID, country<<1|sex, false, async)

	ret.Joined = src.Joined.Int()
	sas.JoinedYear.Set(ret.ID, db.DataEntry(uint16(src.Joined.Time().Year())), false, async)

	ret.PremiumStart = src.Premium.Start.Int()
	ret.PremiumFinish = src.Premium.Finish.Int()

	premium := db.DataEntry(0)
	if ret.PremiumStart != db.NullTime && ret.PremiumFinish != db.NullTime &&
		ret.PremiumStart <= db.CurrentTime && db.CurrentTime <= ret.PremiumFinish {
		premium = 1
	}

	intrs := make([]db.DataEntry, len(src.Interests))
	intrsExt := make([]db.DataEntry, len(src.Interests))
	for i, interest := range src.Interests {
		intr := sas.Interests.ToDictonary(interest)
		intrs[i] = intr
		intrsExt[i] = ((sex<<3)|(premium<<2)|status)*100 + intr
	}
	sas.Interests.SetEntrySet(ret.ID, intrs, false, async)
	sas.SexPremiumStatusInterest.SetEntrySet(ret.ID, intrsExt, false, async)

	likes = make([]db.Like, len(src.Likes))
	for i, like := range src.Likes {
		likes[i] = db.NewLike(
			ret.ID,
			db.IDAcc(like.ID),
			like.Stamp.Int(),
		)
	}
	return
}

func ConvertAccountUpdateSmall(src Account, acc db.SmallAccount, likes []db.Like, sas *db.SmallAccounts) (db.SmallAccount, []db.Like) {

	ret := acc

	ret.ID = db.IDAcc(src.ID)
	if len(src.Email) > 0 {
		i := strings.IndexByte(src.Email, '@')
		if i >= 0 && i < (len(src.Email)-1) {
			sas.Domain.SetString(ret.ID, src.Email[i+1:], true, false)
		}
		dict.DictonaryEml.Put(src.ID, src.Email)
	}
	if len(src.FirstName) > 0 {
		sas.FirstName.SetString(ret.ID, src.FirstName, true, false)
	}
	if len(src.SecondName) > 0 {
		sas.SecondName.SetString(ret.ID, src.SecondName, true, false)
	}
	if len(src.Phone) > 0 {
		ret.Phone = src.Phone
		phc := RePhoneCode.FindString(src.Phone)
		if len(phc) > 2 {
			sas.PhoneCode.SetString(ret.ID, phc[1:len(phc)-1], true, false)
		} else {
			sas.PhoneCode.SetString(ret.ID, "", true, false)
		}
	}
	if len(src.Birth) > 0 {
		ret.Birth = src.Birth.Int()
		sas.BirthYear.Set(ret.ID, db.DataEntry(uint16(src.Birth.Time().Year())), true, false)
	}

	var status, city, country, sex, premium db.DataEntry
	var interests []db.DataEntry

	var updExtSex, updExtStatus, updExtCity, updExtCountry, updExtPremium, updExtInterests bool

	if len(src.Sex) > 0 {
		sex = db.DataEntry(0)
		if src.Sex == "m" {
			sex = db.DataEntry(1)
		}
		sas.Sex.Set(ret.ID, sex, true, false)
		ret.Sex = src.Sex[0]
		updExtSex = true
	}

	if len(src.Status) > 0 {
		status = sas.Status.ToDictonary(src.Status)
		sas.Status.Set(ret.ID, status, true, false)
		updExtStatus = true
	}

	if len(src.Country) > 0 {
		country = sas.Country.ToDictonary(src.Country)
		sas.Country.Set(ret.ID, country, true, false)
		updExtCountry = true
	}

	if len(src.City) > 0 {
		city = sas.City.ToDictonary(src.City)
		sas.City.Set(ret.ID, city, true, false)
		updExtCity = true
	}

	if len(src.Premium.Start) > 0 {
		ret.PremiumStart = src.Premium.Start.Int()
		updExtPremium = true
	}

	if len(src.Premium.Finish) > 0 {
		ret.PremiumFinish = src.Premium.Finish.Int()
		updExtPremium = true
	}

	if src.Interests != nil {
		for _, interest := range src.Interests {
			idx := sas.Interests.ToDictonary(interest)
			interests = append(interests, idx)
		}
		sas.Interests.SetEntrySet(ret.ID, interests, true, false)
		updExtInterests = true
	}

	// ext

	if updExtStatus || updExtCity {
		if status == 0 {
			status = sas.Status.Get(ret.ID)
		}
		if city == 0 {
			city = sas.City.Get(ret.ID)
		}
		sas.StatusCity.Set(ret.ID, city<<2|status, true, false)
	}

	if updExtStatus || updExtCountry {
		if status == 0 {
			status = sas.Status.Get(ret.ID)
		}
		if country == 0 {
			country = sas.Country.Get(ret.ID)
		}
		sas.StatusCountry.Set(ret.ID, country<<2|status, true, false)
	}

	if updExtSex || updExtCity {
		if len(src.Sex) == 0 {
			sex = sas.Sex.Get(ret.ID)
		}
		if city == 0 {
			city = sas.City.Get(ret.ID)
		}
		sas.SexCity.Set(ret.ID, city<<1|sex, true, false)
	}

	if updExtSex || updExtCountry {
		if len(src.Sex) == 0 {
			sex = sas.Sex.Get(ret.ID)
		}
		if country == 0 {
			country = sas.Country.Get(ret.ID)
		}
		sas.SexCountry.Set(ret.ID, country<<1|sex, true, false)
	}

	if updExtSex || updExtPremium || updExtStatus || updExtInterests {
		if len(src.Sex) == 0 {
			sex = sas.Sex.Get(ret.ID)
		}
		if status == 0 {
			status = sas.Status.Get(ret.ID)
		}
		premium = db.DataEntry(0)
		if ret.PremiumStart != db.NullTime && ret.PremiumFinish != db.NullTime &&
			ret.PremiumStart <= db.CurrentTime && db.CurrentTime <= ret.PremiumFinish {
			premium = 1
		}
		if interests == nil {
			interests = sas.Interests.GetSet(ret.ID)
		}
		intrsExt := make([]db.DataEntry, len(interests))
		for i, interest := range interests {
			intrsExt[i] = ((sex<<3)|(premium<<2)|status)*100 + interest
		}
		sas.SexPremiumStatusInterest.SetEntrySet(ret.ID, intrsExt, true, false)
	}

	if len(src.Joined) > 0 {
		ret.Joined = src.Joined.Int()
		sas.JoinedYear.Set(ret.ID, db.DataEntry(uint16(src.Joined.Time().Year())), true, false)
	}
	for _, like := range src.Likes {
		likes = append(likes, db.NewLike(
			ret.ID,
			db.IDAcc(like.ID),
			like.Stamp.Int(),
		))
	}

	return ret, likes
}

func WriteSmallAccountJSON(w io.Writer, sas *db.SmallAccounts, acc db.SmallAccount, fields JSONMask) { // likes []db.Like
	jw := Writer{Buffer: w}
	jw.RawString(`{"id":`)
	jw.Uint32(uint32(acc.ID))
	jw.RawString(`,"email":`)
	eml := dict.DictonaryEml.Get(uint32(acc.ID))

	jw.String(eml)

	if fields&M_fname != 0 {
		v := sas.FirstName.GetString(acc.ID)
		if (fields != MaskRecommend && fields != MaskSuggest) || len(v) > 0 {
			jw.RawString(`,"fname":`)
			jw.String(v)
		}
	}
	if fields&M_sname != 0 {
		v := sas.SecondName.GetString(acc.ID)
		if (fields != MaskRecommend && fields != MaskSuggest) || len(v) > 0 {
			jw.RawString(`,"sname":`)
			jw.String(v)
		}
	}
	if fields&M_phone != 0 {
		jw.RawString(`,"phone":`)
		v := acc.Phone
		jw.String(v)
	}
	if fields&M_sex != 0 {
		jw.RawString(`,"sex":"`)
		jw.RawByte(acc.Sex)
		jw.RawByte('"')
	}
	if fields&M_birth != 0 {
		jw.RawString(`,"birth":`)
		if acc.Birth != db.NullTime {
			jw.Int32(acc.Birth)
		} else {
			jw.RawString("null")
		}
	}
	if fields&M_country != 0 {
		jw.RawString(`,"country":`)
		v := sas.Country.GetString(acc.ID)
		jw.String(v)
	}
	if fields&M_city != 0 {
		jw.RawString(`,"city":`)
		v := sas.City.GetString(acc.ID)
		jw.String(v)
	}
	if fields&M_joined != 0 {
		jw.RawString(`,"joined":`)
		if acc.Joined != db.NullTime {
			jw.Int32(acc.Joined)
		} else {
			jw.RawString("null")
		}
	}
	if fields&M_status != 0 {
		jw.RawString(`,"status":`)
		v := sas.Status.GetString(acc.ID)
		jw.String(v)
	}
	if fields&M_premium != 0 {
		if fields == MaskRecommend {
			if acc.PremiumStart != db.NullTime && acc.PremiumFinish != db.NullTime {
				jw.RawString(`,"premium":{"start":`)
				jw.Int32(acc.PremiumStart)
				jw.RawString(`,"finish":`)
				jw.Int32(acc.PremiumFinish)
				jw.RawByte('}')
			}
		} else {
			jw.RawString(`,"premium":{"start":`)
			if acc.PremiumStart != db.NullTime {
				jw.Int32(acc.PremiumStart)
			} else {
				jw.RawString("null")
			}
			jw.RawString(`,"finish":`)
			if acc.PremiumFinish != db.NullTime {
				jw.Int32(acc.PremiumFinish)
			} else {
				jw.RawString("null")
			}
			jw.RawByte('}')
		}
	}

	jw.RawByte('}')
}
