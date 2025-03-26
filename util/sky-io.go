package util

import (
	"encoding/json"
	"os"
)

// loadFile reads a JSON file and decodes its content into a slice of type T.
func LoadJsonFile[T any](filename string) ([]T, error) {
	// Open the file
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer func(file *os.File) {
		err := file.Close()
		if err != nil {

		}
	}(file)

	// Initialize the JSON decoder
	decoder := json.NewDecoder(file)

	// Read the opening delimiter of the JSON array
	if _, err := decoder.Token(); err != nil {
		return nil, err
	}

	var result []T

	// Iterate over the array elements
	for decoder.More() {
		var item T
		// Decode each array element into item
		if err := decoder.Decode(&item); err != nil {
			return nil, err
		}
		result = append(result, item)
	}

	// Ensure the closing delimiter of the JSON array is read
	if _, err := decoder.Token(); err != nil {
		return nil, err
	}

	return result, nil
}

// JsonDumps Dumps serializes an object into a JSON string (like Python's json.dumps)
func JsonDumps(data interface{}, pretty bool) (string, error) {
	var jsonBytes []byte
	var err error

	if pretty {
		jsonBytes, err = json.MarshalIndent(data, "", "  ") // Pretty format
	} else {
		jsonBytes, err = json.Marshal(data) // Compact format
	}

	if err != nil {
		return "", err
	}
	return string(jsonBytes), nil
}
