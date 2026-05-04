package github

import (
	"fmt"
	"net/http"
)

// ResponseHandler is a function type that takes an HTTP response and returns a parsed result and an error.
type ResponseHandler[T any] func(res *http.Response) (T, error)

// CreateStatusHandler returns a ResponseHandler that checks the HTTP status code
// and handles API errors appropriately.
func CreateStatusHandler[T any](decoder ResponseDecoder[T]) ResponseHandler[T] {
	return func(res *http.Response) (T, error) {
		var data T

		switch {
		case res.StatusCode == http.StatusOK:
			data, err := decoder(res)
			if err != nil {
				return data, &DecodingError{Err: err}
			}
			return data, nil

		case res.StatusCode == http.StatusNotModified:
			return data, nil

		case res.StatusCode >= 400:
			apiErr, err := jsonDecoder[APIError](res)
			if err != nil {
				return data, &DecodingError{Err: fmt.Errorf("api error with status %d (could not decode body)", res.StatusCode)}
			}
			apiErr.StatusCode = res.StatusCode
			return data, &apiErr

		default:
			return data, &UnexpectedStatusError{StatusCode: res.StatusCode}
		}
	}
}
