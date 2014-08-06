package common

import (
	"encoding/json"
)

// KeyMember is used by the Score method, and other places internally. It's
// probably not useful to external users.
type KeyMember struct {
	Key    string
	Member string
}

// KeyScoreMember represents one entry in a ZSET.
type KeyScoreMember struct {
	Key    string
	Score  float64
	Member string
}

// Cursor generates a cursor for this KeyScoreMember.
func (ksm KeyScoreMember) Cursor() Cursor {
	return Cursor{
		Score:  ksm.Score,
		Member: ksm.Member,
	}
}

// jsonKeyScoreMember is used internally by MarshalJSON and UnmarshalJSON.
type jsonKeyScoreMember struct {
	Key    []byte  `json:"key"`
	Score  float64 `json:"score"`
	Member []byte  `json:"member"`
}

// MarshalJSON makes sure that the strings in KeyScoreMember are
// marshalled properly as byte sequences (i.e. base64 encoded in the
// JSON data; the JSON string encoding is neither efficient nor
// reliable for arbitrary byte sequences).
func (ksm KeyScoreMember) MarshalJSON() ([]byte, error) {
	return json.Marshal(&jsonKeyScoreMember{
		Key:    []byte(ksm.Key),
		Score:  ksm.Score,
		Member: []byte(ksm.Member),
	})
}

// UnmarshalJSON makes sure that the strings in KeyScoreMember are
// unmarshalled properly from byte sequences (i.e. base64 encoded in
// the JSON data; the JSON string encoding is neither efficient nor
// reliable for arbitrary byte sequences).
func (ksm *KeyScoreMember) UnmarshalJSON(data []byte) error {
	var jsonKSM jsonKeyScoreMember
	err := json.Unmarshal(data, &jsonKSM)
	if err == nil {
		ksm.Key = string(jsonKSM.Key)
		ksm.Score = jsonKSM.Score
		ksm.Member = string(jsonKSM.Member)
	}
	return err
}
