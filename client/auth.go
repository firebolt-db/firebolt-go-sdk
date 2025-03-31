package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/astaxie/beego/cache"
	"github.com/firebolt-db/firebolt-go-sdk/errors"
	"github.com/firebolt-db/firebolt-go-sdk/logging"
)

const AuthAudienceValue = "https://api.firebolt.io"

type AuthenticationResponse struct {
	AccessToken  string `json:"access_token"`
	RefreshToken string `json:"refresh_token"`
	ExpiresIn    int    `json:"expires_in"`
	TokenType    string `json:"token_type"`
	Scope        string `json:"scope"`
}

var tokenCache cache.Cache

func init() {
	var err error
	tokenCache, err = cache.NewCache("memory", `{}`)
	if err != nil {
		logging.Infolog.Println(fmt.Errorf("could not create memory cache to store access tokens: %v", err))
	}
}

// jsonStrictUnmarshall unmarshalls json into object, and returns an error
// if some fields are missing, or extra fields are present
func jsonStrictUnmarshall(data []byte, v interface{}) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	return decoder.Decode(v)
}

// getAccessTokenUsernamePassword gets an access token from the cache when it is available in the cache or from the server when it is not available in the cache
func getAccessTokenUsernamePassword(username string, password string, apiEndpoint string, userAgent string) (string, error) {
	cachedToken := getCachedAccessToken(username, apiEndpoint)
	if len(cachedToken) > 0 {
		return cachedToken, nil
	} else {
		var loginUrl string
		var contentType string
		var body string
		var err error
		loginUrl, contentType, body, err = prepareUsernamePasswordLogin(username, password)
		if err != nil {
			return "", err
		}
		logging.Infolog.Printf("Start authentication into '%s' using '%s'", apiEndpoint, loginUrl)
		resp := DoHttpRequest(requestParameters{context.TODO(), "", "POST", apiEndpoint + loginUrl, userAgent, nil, body, contentType})
		if resp.statusCode == 400 || resp.statusCode == 403 {
			return "", errors.Wrap(errors.AuthenticationError, resp.err)
		} else if resp.err != nil {
			fmt.Printf("Status code: %d\n", resp.statusCode)
			return "", errors.ConstructNestedError("authentication request failed", resp.err)
		}

		content, err := resp.Content()
		if err != nil {
			return "", errors.ConstructNestedError("error during reading response content", err)
		}

		var authResp AuthenticationResponse
		if err = jsonStrictUnmarshall(content, &authResp); err != nil {
			return "", errors.ConstructNestedError("failed to unmarshal authentication response with error", err)
		}
		logging.Infolog.Printf("Authentication was successful")
		if tokenCache != nil {
			err = tokenCache.Put(getCacheKey(username, apiEndpoint), authResp.AccessToken, time.Duration(authResp.ExpiresIn)*time.Millisecond)
			if err != nil {
				logging.Infolog.Println(fmt.Errorf("failed to cache access token: %v", err))
			}
		}
		return authResp.AccessToken, nil
	}
}

// prepareUsernamePasswordLogin returns the loginUrl, contentType and body needed to query an access token using a username and a password
func prepareUsernamePasswordLogin(username string, password string) (string, string, string, error) {
	var authUrl = UsernamePasswordURLSuffix
	values := map[string]string{"username": username, "password": password}
	jsonData, err := json.Marshal(values)
	if err != nil {
		return "", "", "", errors.ConstructNestedError("error during json marshalling", err)
	} else {
		return authUrl, ContentTypeJSON, string(jsonData), nil
	}
}

// getAccessTokenServiceAccount gets an access token from the cache when it is available in the cache or from the server when it is not available in the cache
func getAccessTokenServiceAccount(clientId string, clientSecret string, apiEndpoint string, userAgent string) (string, error) {
	cachedToken := getCachedAccessToken(clientId, apiEndpoint)
	if len(cachedToken) > 0 {
		return cachedToken, nil
	} else {
		var loginUrl string
		var contentType string
		var body string
		var err error

		loginUrl, contentType, body = prepareServiceAccountLogin(clientId, clientSecret, AuthAudienceValue)
		authEndpoint, err := getServiceAccountAuthEndpoint(apiEndpoint)
		if err != nil {
			return "", errors.ConstructNestedError("error building auth endpoint", err)
		}
		logging.Infolog.Printf("Start authentication into '%s' using '%s'", authEndpoint, loginUrl)
		resp := DoHttpRequest(requestParameters{context.TODO(), "", "POST", authEndpoint + loginUrl, userAgent, nil, body, contentType})
		if resp.statusCode == 401 {
			return "", errors.Wrap(errors.AuthenticationError, resp.err)
		} else if resp.err != nil {
			return "", errors.ConstructNestedError("authentication request failed", resp.err)
		}

		content, err := resp.Content()
		if err != nil {
			return "", errors.ConstructNestedError("error during reading response content", err)
		}

		var authResp AuthenticationResponse
		if err = jsonStrictUnmarshall(content, &authResp); err != nil {
			return "", errors.ConstructNestedError("failed to unmarshal authentication response with error", err)
		}
		logging.Infolog.Printf("Authentication was successful")
		if tokenCache != nil {
			err = tokenCache.Put(getCacheKey(clientId, apiEndpoint), authResp.AccessToken, time.Duration(authResp.ExpiresIn)*time.Millisecond)
			if err != nil {
				logging.Infolog.Println(fmt.Errorf("failed to cache access token: %v", err))
			}
		}
		return authResp.AccessToken, nil
	}
}

// prepareServiceAccountLogin returns the loginUrl, contentType and body needed to query an access token using a client id and a client secret
func prepareServiceAccountLogin(clientId, clientSecret, audience string) (string, string, string) {
	var authUrl = ServiceAccountLoginURLSuffix
	form := url.Values{}
	form.Add("client_id", clientId)
	form.Add("client_secret", clientSecret)
	form.Add("grant_type", "client_credentials")
	form.Add("audience", audience)
	var body = form.Encode()
	return authUrl, ContentTypeForm, body
}

// getServiceAccountAuthEndpoint replaces 'api' with 'id' in the beginning of the endpoint url,
func getServiceAccountAuthEndpoint(apiEndpoint string) (string, error) {
	u, err := url.Parse(apiEndpoint)
	if err != nil {
		return "", errors.ConstructNestedError("error parsing api endpoint", err)
	}
	s := strings.Split(u.Host, ".")
	if s[0] != "api" {
		// We expect an apiEndpoint to be of format api.<env>.firebolt.io
		// Since we got something else, assume it's a test
		return apiEndpoint, nil
	}
	s[0] = "id"
	u.Host = strings.Join(s, ".")
	return u.String(), nil
}

// deleteAccessTokenFromCache deletes an access token from the cache if available
func deleteAccessTokenFromCache(username, apiEndpoint string) {
	if tokenCache != nil {
		err := tokenCache.Delete(getCacheKey(username, apiEndpoint))
		if err != nil {
			logging.Infolog.Println(fmt.Errorf("could not remove token from the memory cache: %v", err))
		}
	}
}

// getCacheKey calculates an access token key using the username and the apiEndpoint provided
func getCacheKey(clientID, apiEndpoint string) string {
	return clientID + apiEndpoint
}

// getCachedAccessToken returns a cached access token or empty when a token could not be found
func getCachedAccessToken(clientID, apiEndpoint string) string {
	if tokenCache != nil {
		var cachedToken = tokenCache.Get(getCacheKey(clientID, apiEndpoint))
		if cachedToken != nil {
			return fmt.Sprint(cachedToken)
		}
	}
	return ""
}
