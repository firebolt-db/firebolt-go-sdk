package client

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	contextUtils "github.com/firebolt-db/firebolt-go-sdk/context"

	errorUtils "github.com/firebolt-db/firebolt-go-sdk/errors"
	"github.com/firebolt-db/firebolt-go-sdk/logging"
)

type Response struct {
	body       io.ReadCloser
	content    []byte
	statusCode int
	headers    http.Header
	err        error
}

func MakeResponse(body io.ReadCloser, statusCode int, headers http.Header, err error) *Response {
	response := &Response{body, nil, statusCode, headers, err}

	if response.err == nil && (statusCode < 200 || statusCode >= 300) {
		if err := checkErrorResponse(response); err != nil {
			response.err = errorUtils.ConstructNestedError("request returned an error", err)
		} else if statusCode == 500 {
			response.err = errors.New(string(response.content))
		} else {
			response.err = errorUtils.ConstructNestedError("request returned non ok status code", errors.New(string(response.content)))
		}
	}
	return response
}

func (r *Response) Body() io.ReadCloser {
	return r.body
}

func (r *Response) Content() ([]byte, error) {
	var err error
	if r.content == nil {
		if r.body == nil {
			r.content = []byte{}
		} else {
			r.content, err = io.ReadAll(r.body)
			if err != nil {
				err = r.body.Close()
			}
		}
	}
	return r.content, err
}

func (r *Response) IsAsyncResponse() bool {
	return r.statusCode == 202
}

// Collect arguments for request function
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

// checkErrorResponse, checks whether error Response is returned instead of a desired Response.
func checkErrorResponse(response *Response) error {
	// ErrorResponse definition of any Response with some error
	type ErrorResponse struct {
		Error   string        `json:"error"`
		Code    int           `json:"code"`
		Message string        `json:"message"`
		Details []interface{} `json:"details"`
	}

	content, err := response.Content()
	if err != nil {
		return errorUtils.ConstructNestedError("error during reading response from the server", err)
	}

	var errorResponse ErrorResponse

	if err := json.Unmarshal(content, &errorResponse); err == nil && errorResponse.Code != 0 {
		// return error only if error Response was
		// unmarshalled correctly and error code is not zero
		return errors.New(errorResponse.Message)
	}
	return nil
}

func extractAdditionalHeaders(ctx context.Context) map[string]string {
	additionalHeaders, ok := contextUtils.GetAdditionalHeaders(ctx)
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

// DoHttpRequest sends a request using "POST" or "GET" method on a specified url
// additionally it passes the parameters and a bodyStr as a payload
// if accessToken is passed, it is used for authorization
// returns Response struct
func DoHttpRequest(reqParams requestParameters) *Response {
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
		return MakeResponse(nil, 0, nil, errorUtils.ConstructNestedError("error during a request execution", err))
	}

	return MakeResponse(resp.Body, resp.StatusCode, resp.Header, nil)
}

// MakeCanonicalUrl checks whether url starts with https:// and if not prepends it
func MakeCanonicalUrl(url string) string {
	if strings.HasPrefix(url, "https://") || strings.HasPrefix(url, "http://") {
		return url
	} else {
		return fmt.Sprintf("https://%s", url)
	}
}
