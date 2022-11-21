package fireboltgosdk

import (
	"context"
	"encoding/json"
	"net/url"
	"strings"
	"time"

	"github.com/jellydator/ttlcache/v3"
)

type AuthenticationResponse struct {
	AccessToken  string `json:"access_token"`
	RefreshToken string `json:"refresh_token"`
	ExpiresIn    int    `json:"expires_in"`
	TokenType    string `json:"token_type"`
	Scope        string `json:"scope"`
}

var cache = ttlcache.New[string, string]()

// Authenticate sends an authentication request, and returns a newly constructed client object
func Authenticate(username, password, apiEndpoint string) (*Client, error) {
	var loginUrl string
	var contentType string
	var body string
	var err error

	userAgent := ConstructUserAgentString()
	cached := cache.Get(getCacheKey(username, apiEndpoint))
	if cached != nil {
		infolog.Printf("Found auth token from cache")
		return &Client{Username: username, Password: password, ApiEndpoint: apiEndpoint, UserAgent: userAgent}, nil
	} else {
		if strings.Contains(username, "@") {
			loginUrl, contentType, body, err = prepareUsernamePasswordLogin(username, password)
			if err != nil {
				return nil, err
			}
		} else {
			loginUrl, contentType, body = prepareServiceAccountLogin(username, password)
		}
		infolog.Printf("Start authentication into '%s' using '%s'", apiEndpoint, loginUrl)

		resp, err, _ := request(context.TODO(), "", "POST", apiEndpoint+loginUrl, userAgent, nil, body, contentType)
		if err != nil {
			return nil, ConstructNestedError("authentication request failed", err)
		}

		var authResp AuthenticationResponse
		if err = jsonStrictUnmarshall(resp, &authResp); err != nil {
			return nil, ConstructNestedError("failed to unmarshal authentication response with error", err)
		}

		infolog.Printf("Authentication was successful")
		cache.Set(getCacheKey(username, apiEndpoint), authResp.AccessToken, time.Duration(authResp.ExpiresIn)*time.Millisecond)
		return &Client{Username: username, Password: password, ApiEndpoint: apiEndpoint, UserAgent: userAgent}, nil
	}
}

func prepareUsernamePasswordLogin(username string, password string) (string, string, string, error) {
	var authUrl = UsernamePasswordURLSuffix
	//var values = map[string]string{"username": username, "password": password}
	var contentType = "application/json"
	values := map[string]string{"username": username, "password": password}
	jsonData, err := json.Marshal(values)
	if err != nil {
		return "", "", "", ConstructNestedError("error during json marshalling", err)
	} else {
		return authUrl, contentType, string(jsonData), nil
	}
}

func prepareServiceAccountLogin(username, password string) (string, string, string) {
	var authUrl = ServiceAccountLoginURLSuffix
	form := url.Values{}
	form.Add("client_id", username)
	form.Add("client_secret", password)
	form.Add("grant_type", "client_credentials")
	var contentType = "application/x-www-form-urlencoded"
	var body = form.Encode()
	return authUrl, contentType, body
}

func getCacheKey(username, apiEndpoint string) string {
	return username + apiEndpoint
}

func getCachedToken(username, apiEndpoint string) string {
	cached := cache.Get(getCacheKey(username, apiEndpoint))
	if cached != nil {
		return cached.Value()
	} else {
		return ""
	}
}

func DeleteTokenFromCache(username, apiEndpoint string) {
	cache.Delete(getCacheKey(username, apiEndpoint))
}
