package common

import (
	"encoding/json"
)

// KeyScoreMember represents one entry in a ZSET.
type KeyScoreMember struct {
	Key    string
	Score  float64
	Member string
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
	return json.Marshal(&jsonKeyScoreMember{[]byte(ksm.Key), ksm.Score, []byte(ksm.Member)})
}

// UnmarshalJSON makes sure that the strings in KeyScoreMember are
// unmarshalled properly from byte sequences (i.e. base64 encoded in
// the JSON data; the JSON string encoding is neither efficient nor
// reliable for arbitrary byte sequences).
func (ksm *KeyScoreMember) UnmarshalJSON(data []byte) error {
	var jsonKsm jsonKeyScoreMember
	err := json.Unmarshal(data, &jsonKsm)
	if err == nil {
		ksm.Key = string(jsonKsm.Key)
		ksm.Score = jsonKsm.Score
		ksm.Member = string(jsonKsm.Member)
	}
	return err
}

// KeyScoreMembers implements sort.Sort methods on a slice of KeyScoreMember.
type KeyScoreMembers []KeyScoreMember

// Len implements sort.Sort.
func (a KeyScoreMembers) Len() int { return len(a) }

// Less implements sort.Sort.
func (a KeyScoreMembers) Less(i, j int) bool { return a[i].Score > a[j].Score }

// Swap implements sort.Sort.
func (a KeyScoreMembers) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
