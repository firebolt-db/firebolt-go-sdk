package client

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	errors2 "github.com/firebolt-db/firebolt-go-sdk/errors"
	"github.com/firebolt-db/firebolt-go-sdk/logging"
	"github.com/firebolt-db/firebolt-go-sdk/types"
)

type Response struct {
	data       []byte
	statusCode int
	headers    http.Header
	err        error
}

// Collect arguments for DoHttpRequest function
type requestParameters struct {
	ctx         context.Context
	accessToken string
	method      string
	url         string
	userAgent   string
	params      map[string]string
	bodyStr     string
	contentType string
}

type ContextKey string

// checkErrorResponse, checks whether error Response is returned instead of a desired Response.
func checkErrorResponse(response []byte) error {
	// ErrorResponse definition of any Response with some error
	type ErrorResponse struct {
		Error   string        `json:"error"`
		Code    int           `json:"code"`
		Message string        `json:"message"`
		Details []interface{} `json:"details"`
	}

	var errorResponse ErrorResponse

	if err := json.Unmarshal(response, &errorResponse); err == nil && errorResponse.Code != 0 {
		// return error only if error Response was
		// unmarshalled correctly and error code is not zero
		return errors.New(errorResponse.Message)
	}
	return nil
}

func extractAdditionalHeaders(ctx context.Context) map[string]string {
	additionalHeaders, ok := ctx.Value(ContextKey("additionalHeaders")).(map[string]string)
	if ok {
		// only take headers that start with Firebolt- prefix
		filteredHeaders := make(map[string]string)
		for key, value := range additionalHeaders {
			if strings.HasPrefix(key, "Firebolt-") {
				filteredHeaders[key] = value
			}
		}
		return filteredHeaders
	}
	return map[string]string{}
}

// DoHttpRequest sends a DoHttpRequest using "POST" or "GET" method on a specified url
// additionally it passes the parameters and a bodyStr as a payload
// if accessToken is passed, it is used for authorization
// returns Response and an error
func DoHttpRequest(reqParams requestParameters) Response {
	req, _ := http.NewRequestWithContext(reqParams.ctx, reqParams.method, MakeCanonicalUrl(reqParams.url), strings.NewReader(reqParams.bodyStr))

	// adding sdk usage tracking
	req.Header.Set("User-Agent", reqParams.userAgent)

	// add protocol version header
	req.Header.Set(protocolVersionHeader, protocolVersion)

	if len(reqParams.accessToken) > 0 {
		var bearer = "Bearer " + reqParams.accessToken
		req.Header.Add("Authorization", bearer)
	}

	if len(reqParams.contentType) > 0 {
		req.Header.Set("Content-Type", reqParams.contentType)
	}

	// add additional headers from context
	for key, value := range extractAdditionalHeaders(reqParams.ctx) {
		req.Header.Set(key, value)
	}

	q := req.URL.Query()
	for key, value := range reqParams.params {
		q.Add(key, value)
	}
	req.URL.RawQuery = q.Encode()

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		logging.Infolog.Println(err)
		return Response{nil, 0, nil, errors2.ConstructNestedError("error during a DoHttpRequest execution", err)}
	}

	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		logging.Infolog.Println(err)
		return Response{nil, 0, nil, errors2.ConstructNestedError("error during reading a DoHttpRequest Response", err)}
	}
	// Error might be in the Response body, despite the status code 200
	errorResponse := struct {
		Errors []types.ErrorDetails `json:"errors"`
	}{}
	if err = json.Unmarshal(body, &errorResponse); err == nil {
		if errorResponse.Errors != nil {
			return Response{nil, resp.StatusCode, nil, errors2.NewStructuredError(errorResponse.Errors)}
		}
	}

	if !(resp.StatusCode >= 200 && resp.StatusCode < 300) {
		if err = checkErrorResponse(body); err != nil {
			return Response{nil, resp.StatusCode, nil, errors2.ConstructNestedError("DoHttpRequest returned an error", err)}
		}
		if resp.StatusCode == 500 {
			// this is a database error
			return Response{nil, resp.StatusCode, nil, fmt.Errorf("%s", string(body))}
		}
		return Response{nil, resp.StatusCode, nil, fmt.Errorf("DoHttpRequest returned non ok status code: %d, %s", resp.StatusCode, string(body))}
	}

	return Response{body, resp.StatusCode, resp.Header, nil}
}

// MakeCanonicalUrl checks whether url starts with https:// and if not prepends it
func MakeCanonicalUrl(url string) string {
	if strings.HasPrefix(url, "https://") || strings.HasPrefix(url, "http://") {
		return url
	} else {
		return fmt.Sprintf("https://%s", url)
	}
}
