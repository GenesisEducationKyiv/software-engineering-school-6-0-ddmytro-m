//go:build unit

package github

import (
	"errors"
	"net/http"
	"strconv"
	"testing"
)

func TestCreateStatusHandler_200_ValidJSON(t *testing.T) {
	handler := CreateStatusHandler(jsonDecoder[LatestRelease])
	res := fakeResponse(http.StatusOK,
		`{"id":1,"tag_name":"v1.2.3","html_url":"https://github.com/x/y/releases/tag/v1.2.3"}`,
		nil,
	)

	data, err := handler(res)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if data.ID != 1 {
		t.Errorf("ID = %d, want 1", data.ID)
	}
	if data.TagName != "v1.2.3" {
		t.Errorf("TagName = %q, want v1.2.3", data.TagName)
	}
	if data.URL != "https://github.com/x/y/releases/tag/v1.2.3" {
		t.Errorf("URL = %q, want https://github.com/x/y/releases/tag/v1.2.3", data.URL)
	}
}

func TestCreateStatusHandler_200_MalformedJSON_ReturnsDecodingError(t *testing.T) {
	handler := CreateStatusHandler(jsonDecoder[LatestRelease])
	res := fakeResponse(http.StatusOK, "{not valid json", nil)

	data, err := handler(res)
	var de *DecodingError
	if !errors.As(err, &de) {
		t.Errorf("expected *DecodingError, got %T: %v", err, err)
	}
	if data.ID != 0 {
		t.Errorf("ID = %d, want 0", data.ID)
	}
}

func TestCreateStatusHandler_304_ReturnsZeroValueNoError(t *testing.T) {
	handler := CreateStatusHandler(jsonDecoder[LatestRelease])
	res := fakeResponse(http.StatusNotModified, "", nil)

	data, err := handler(res)
	if err != nil {
		t.Fatalf("unexpected error on 304: %v", err)
	}
	if data.TagName != "" {
		t.Errorf("expected zero-value data on 304, got %+v", data)
	}
}

func TestCreateStatusHandler_4xx_ReturnsAPIError(t *testing.T) {
	for _, code := range []int{401, 403, 404, 422, 429} {
		t.Run(strconv.Itoa(code), func(t *testing.T) {
			handler := CreateStatusHandler(jsonDecoder[LatestRelease])
			res := fakeResponse(code, `{"message":"some api error"}`, nil)

			_, err := handler(res)
			var apiErr *APIError
			if !errors.As(err, &apiErr) {
				t.Fatalf("expected *APIError, got %T: %v", err, err)
			}
			if apiErr.StatusCode != code {
				t.Errorf("APIError.StatusCode = %d, want %d", apiErr.StatusCode, code)
			}
			if apiErr.Message != "some api error" {
				t.Errorf("APIError.Message = %q", apiErr.Message)
			}
		})
	}
}

func TestCreateStatusHandler_4xx_MalformedErrorBody_ReturnsDecodingError(t *testing.T) {
	handler := CreateStatusHandler(jsonDecoder[LatestRelease])
	res := fakeResponse(http.StatusNotFound, "not json", nil)

	_, err := handler(res)
	var de *DecodingError
	if !errors.As(err, &de) {
		t.Errorf("expected *DecodingError for unparseable error body, got %T: %v", err, err)
	}
}

func TestCreateStatusHandler_UnexpectedStatus(t *testing.T) {
	for _, code := range []int{201, 202, 301, 302} {
		t.Run(strconv.Itoa(code), func(t *testing.T) {
			handler := CreateStatusHandler(jsonDecoder[LatestRelease])
			res := fakeResponse(code, "", nil)

			_, err := handler(res)
			var use *UnexpectedStatusError
			if !errors.As(err, &use) {
				t.Errorf("expected *UnexpectedStatusError, got %T: %v", err, err)
			}
			if use.StatusCode != code {
				t.Errorf("StatusCode = %d, want %d", use.StatusCode, code)
			}
		})
	}
}
