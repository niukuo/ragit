package refs

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"
)

func DecodeSnapshot(data []byte) (map[string]Hash, error) {
	refs := strings.Split(string(data), "\n")
	refsMap := make(map[string]Hash)

	for _, line := range refs {
		if line == "" {
			continue
		}
		slices := strings.SplitN(line, " ", 3)
		if len(slices) != 2 {
			return nil, fmt.Errorf("invalid line: %s", line)
		}

		if !strings.HasPrefix(slices[1], "refs/") {
			return nil, fmt.Errorf("invalid refs: %s", slices[1])
		}

		if _, ok := refsMap[slices[1]]; ok {
			return nil, fmt.Errorf("dup refs: %s", slices[1])
		}

		hash, err := hex.DecodeString(slices[0])
		if err != nil || len(hash) != 20 {
			return nil, fmt.Errorf("invalid refs hash: %s", slices[0])
		}

		var bhash Hash
		copy(bhash[:], hash)

		refsMap[slices[1]] = bhash
	}

	return refsMap, nil
}

func EncodeSnapshot(refs map[string]Hash) []byte {
	names := make([]string, 0, len(refs))
	for name := range refs {
		names = append(names, name)
	}
	sort.Strings(names)

	var buf bytes.Buffer
	for _, name := range names {
		hash := refs[name]
		fmt.Fprintf(&buf, "%s %s\n", hex.EncodeToString(hash[:]), name)
	}

	return buf.Bytes()
}
