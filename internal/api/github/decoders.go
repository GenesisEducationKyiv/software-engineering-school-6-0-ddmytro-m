package github

import (
	"encoding/json"
	"net/http"
)

type ResponseDecoder[T any] func(*http.Response) (T, error)

func jsonDecoder[T any](res *http.Response) (T, error) {
	var data T
	err := json.NewDecoder(res.Body).Decode(&data)
	return data, err
}
