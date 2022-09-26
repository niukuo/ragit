package refs

import (
	"encoding/json"
	"fmt"
)

type SnapshotData struct {
	Refs    map[string]Hash `json:"refs"`
	Members []Member        `json:"members"`
}

func DecodeSnapshot(data []byte) (*SnapshotData, error) {
	var snapshotData SnapshotData
	if err := json.Unmarshal(data, &snapshotData); err != nil {
		return nil, fmt.Errorf("Unmarshal snapshotData failed, err: %w", err)
	}

	return &snapshotData, nil
}

func EncodeSnapshot(snapshotData *SnapshotData) ([]byte, error) {
	data, err := json.Marshal(snapshotData)
	if err != nil {
		return nil, err
	}

	return data, nil
}
