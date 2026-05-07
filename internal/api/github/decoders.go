package github

import (
	"encoding/json"
	"net/http"
)

// ResponseDecoder is a function type that decodes an HTTP response body into a generic type T.
type ResponseDecoder[T any] func(*http.Response) (T, error)

func jsonDecoder[T any](res *http.Response) (T, error) {
	var data T
	err := json.NewDecoder(res.Body).Decode(&data)
	return data, err
}
