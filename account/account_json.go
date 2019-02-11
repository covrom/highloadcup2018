package account

import (
	json "encoding/json"
	"errors"

	easyjson "github.com/mailru/easyjson"
	jlexer "github.com/mailru/easyjson/jlexer"
	jwriter "github.com/mailru/easyjson/jwriter"
)

var (
	_                    *json.RawMessage
	_                    *jlexer.Lexer
	_                    *jwriter.Writer
	_                    easyjson.Marshaler
	errWrongFieldName    = errors.New("wrong field name")
	errWrongFieldContent = errors.New("wrong field content")
	ErrEmptyID           = errors.New("id is empty")
)

func decodePremium(in *jlexer.Lexer, out *PremiumInterval) {
	isTopLevel := in.IsStart()
	if in.IsNull() {
		if isTopLevel {
			in.Consumed()
		}
		in.Skip()
		return
	}
	in.Delim('{')
	for !in.IsDelim('}') {
		key := in.UnsafeString()
		in.WantColon()
		if in.IsNull() {
			in.Skip()
			in.WantComma()
			continue
		}
		switch key {
		case "start":
			if data := in.Raw(); in.Ok() {
				in.AddError((out.Start).UnmarshalJSON(data))
			}
		case "finish":
			if data := in.Raw(); in.Ok() {
				in.AddError((out.Finish).UnmarshalJSON(data))
			}
		default:
			in.SkipRecursive()
		}
		in.WantComma()
	}
	in.Delim('}')
	if isTopLevel {
		in.Consumed()
	}
}

// UnmarshalJSON supports json.Unmarshaler interface
func (v *PremiumInterval) UnmarshalJSON(data []byte) error {
	r := jlexer.Lexer{Data: data}
	decodePremium(&r, v)
	return r.Error()
}

func decodeOneLike(in *jlexer.Lexer, out *OneLike) {
	isTopLevel := in.IsStart()
	if in.IsNull() {
		if isTopLevel {
			in.Consumed()
		}
		in.Skip()
		return
	}
	in.Delim('{')
	for !in.IsDelim('}') {
		key := in.UnsafeString()
		in.WantColon()
		if in.IsNull() {
			in.Skip()
			in.WantComma()
			continue
		}
		switch key {
		case "id":
			out.ID = uint32(in.Uint32())
		case "ts":
			if data := in.Raw(); in.Ok() {
				in.AddError((out.Stamp).UnmarshalJSON(data))
			}
		default:
			in.SkipRecursive()
		}
		in.WantComma()
	}
	in.Delim('}')
	if isTopLevel {
		in.Consumed()
	}
}

// UnmarshalJSON supports json.Unmarshaler interface
func (v *OneLike) UnmarshalJSON(data []byte) error {
	r := jlexer.Lexer{Data: data}
	decodeOneLike(&r, v)
	return r.Error()
}

func decodeAccounts(in *jlexer.Lexer, out *Accounts) {
	isTopLevel := in.IsStart()
	if in.IsNull() {
		if isTopLevel {
			in.Consumed()
		}
		in.Skip()
		return
	}
	in.Delim('{')
	for !in.IsDelim('}') {
		key := in.UnsafeString()
		in.WantColon()
		if in.IsNull() {
			in.Skip()
			in.WantComma()
			continue
		}
		switch key {
		case "accounts":
			if in.IsNull() {
				in.Skip()
				out.Accounts = nil
			} else {
				in.Delim('[')
				if out.Accounts == nil {
					if !in.IsDelim(']') {
						out.Accounts = make([]Account, 0, 1)
					} else {
						out.Accounts = []Account{}
					}
				} else {
					out.Accounts = (out.Accounts)[:0]
				}
				for !in.IsDelim(']') {
					var v1 Account
					if data := in.Raw(); in.Ok() {
						in.AddError((v1).UnmarshalJSON(data))
					}
					out.Accounts = append(out.Accounts, v1)
					in.WantComma()
				}
				in.Delim(']')
			}
		default:
			in.SkipRecursive()
		}
		in.WantComma()
	}
	in.Delim('}')
	if isTopLevel {
		in.Consumed()
	}
}

// UnmarshalJSON supports json.Unmarshaler interface
func (v *Accounts) UnmarshalJSON(data []byte) error {
	r := jlexer.Lexer{Data: data}
	decodeAccounts(&r, v)
	return r.Error()
}

func decodeAccount(in *jlexer.Lexer, out *Account) {
	var hasID bool
	isTopLevel := in.IsStart()
	if in.IsNull() {
		if isTopLevel {
			in.Consumed()
		}
		in.Skip()
		return
	}
	in.Delim('{')
	for !in.IsDelim('}') {
		key := in.UnsafeString()
		in.WantColon()
		if in.IsNull() {
			in.Skip()
			in.WantComma()
			continue
		}
		switch key {
		case "id":
			out.ID = uint32(in.Uint32())
			hasID = true
		case "email":
			out.Email = string(in.String())
		case "fname":
			out.FirstName = string(in.String())
		case "sname":
			out.SecondName = string(in.String())
		case "phone":
			out.Phone = string(in.String())
		case "sex":
			out.Sex = string(in.String())
		case "birth":
			if data := in.Raw(); in.Ok() {
				in.AddError((out.Birth).UnmarshalJSON(data))
			}
		case "country":
			out.Country = string(in.String())
		case "city":
			out.City = string(in.String())
		case "joined":
			if data := in.Raw(); in.Ok() {
				in.AddError((out.Joined).UnmarshalJSON(data))
			}
		case "status":
			out.Status = string(in.String())
		case "interests":
			if in.IsNull() {
				in.Skip()
				out.Interests = nil
			} else {
				in.Delim('[')
				if out.Interests == nil {
					if !in.IsDelim(']') {
						out.Interests = make([]string, 0, 4)
					} else {
						out.Interests = []string{}
					}
				} else {
					out.Interests = (out.Interests)[:0]
				}
				for !in.IsDelim(']') {
					var v4 string
					v4 = string(in.String())
					out.Interests = append(out.Interests, v4)
					in.WantComma()
				}
				in.Delim(']')
			}
		case "premium":
			if data := in.Raw(); in.Ok() {
				in.AddError((out.Premium).UnmarshalJSON(data))
			}
		case "likes":
			if in.IsNull() {
				in.Skip()
				out.Likes = nil
			} else {
				in.Delim('[')
				if out.Likes == nil {
					if !in.IsDelim(']') {
						out.Likes = make([]OneLike, 0, 2)
					} else {
						out.Likes = []OneLike{}
					}
				} else {
					out.Likes = (out.Likes)[:0]
				}
				for !in.IsDelim(']') {
					var v5 OneLike
					if data := in.Raw(); in.Ok() {
						in.AddError((v5).UnmarshalJSON(data))
					}
					out.Likes = append(out.Likes, v5)
					in.WantComma()
				}
				in.Delim(']')
			}
		default:
			in.AddError(errWrongFieldName)
			in.SkipRecursive()
		}
		in.WantComma()
	}
	in.Delim('}')
	if isTopLevel {
		in.Consumed()
	}
	if !hasID {
		in.AddError(ErrEmptyID)
	}
}

// UnmarshalJSON supports json.Unmarshaler interface
func (v *Account) UnmarshalJSON(data []byte) error {
	r := jlexer.Lexer{Data: data}
	decodeAccount(&r, v)
	return r.Error()
}

func decodeUpdateLikes(in *jlexer.Lexer, out *UpdateLikes) {
	isTopLevel := in.IsStart()
	if in.IsNull() {
		if isTopLevel {
			in.Consumed()
		}
		in.Skip()
		return
	}
	in.Delim('{')
	for !in.IsDelim('}') {
		key := in.UnsafeString()
		in.WantColon()
		if in.IsNull() {
			in.Skip()
			in.WantComma()
			continue
		}
		switch key {
		case "likes":
			if in.IsNull() {
				in.Skip()
				out.Likes = nil
			} else {
				in.Delim('[')
				if out.Likes == nil {
					if !in.IsDelim(']') {
						out.Likes = make([]OneUpdateLike, 0, 1)
					} else {
						out.Likes = []OneUpdateLike{}
					}
				} else {
					out.Likes = (out.Likes)[:0]
				}
				for !in.IsDelim(']') {
					var v1 OneUpdateLike
					if data := in.Raw(); in.Ok() {
						in.AddError((v1).UnmarshalJSON(data))
					}
					out.Likes = append(out.Likes, v1)
					in.WantComma()
				}
				in.Delim(']')
			}
		default:
			in.SkipRecursive()
		}
		in.WantComma()
	}
	in.Delim('}')
	if isTopLevel {
		in.Consumed()
	}
}

// UnmarshalJSON supports json.Unmarshaler interface
func (v *UpdateLikes) UnmarshalJSON(data []byte) error {
	r := jlexer.Lexer{Data: data}
	decodeUpdateLikes(&r, v)
	return r.Error()
}

func decodeOneUpdateLike(in *jlexer.Lexer, out *OneUpdateLike) {
	isTopLevel := in.IsStart()
	if in.IsNull() {
		if isTopLevel {
			in.Consumed()
		}
		in.Skip()
		return
	}
	in.Delim('{')
	for !in.IsDelim('}') {
		key := in.UnsafeString()
		in.WantColon()
		if in.IsNull() {
			in.Skip()
			in.WantComma()
			continue
		}
		switch key {
		case "likee":
			out.Likee = uint32(in.Uint32())
		case "liker":
			out.Liker = uint32(in.Uint32())
		case "ts":
			if data := in.Raw(); in.Ok() {
				in.AddError((out.Stamp).UnmarshalJSON(data))
			}
		default:
			in.SkipRecursive()
		}
		in.WantComma()
	}
	in.Delim('}')
	if isTopLevel {
		in.Consumed()
	}
}

// UnmarshalJSON supports json.Unmarshaler interface
func (v *OneUpdateLike) UnmarshalJSON(data []byte) error {
	r := jlexer.Lexer{Data: data}
	decodeOneUpdateLike(&r, v)
	return r.Error()
}
