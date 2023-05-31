package fireboltgosdk

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/astaxie/beego/cache"
)

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
		infolog.Println(fmt.Errorf("could not create memory cache to store access tokens: %v", err))
	}
}

// Authenticate sends an authentication request, and returns a newly constructed client object
func Authenticate(clientId string, clientSecret string, apiEndpoint string, accountName string) (*Client, error) {
	userAgent := ConstructUserAgentString()
	_, err := getAccessToken(clientId, clientSecret, apiEndpoint, userAgent)
	if err != nil {
		return nil, ConstructNestedError("error while getting access token", err)
	} else {
		return NewClient(clientId, clientSecret, apiEndpoint, userAgent, accountName)
	}
}

// getAccessToken gets an access token from the cache when it is available in the cache or from the server when it is not available in the cache
func getAccessToken(clientId string, clientSecret string, apiEndpoint string, userAgent string) (string, error) {
	cachedToken := getCachedAccessToken(clientId, apiEndpoint)
	if len(cachedToken) > 0 {
		return cachedToken, nil
	} else {
		var loginUrl string
		var contentType string
		var body string
		var err error

		loginUrl, contentType, body = prepareServiceAccountLogin(clientId, clientSecret, "https://api.firebolt.io")
		authEndpoint, err := getAuthEndpoint(apiEndpoint)
		if err != nil {
			return "", ConstructNestedError("error building auth endpoint", err)
		}
		infolog.Printf("Start authentication into '%s' using '%s'", authEndpoint, loginUrl)
		resp, err, _ := request(context.TODO(), "", "POST", authEndpoint+loginUrl, userAgent, nil, body, contentType)
		if err != nil {
			return "", ConstructNestedError("authentication request failed", err)
		}

		var authResp AuthenticationResponse
		if err = jsonStrictUnmarshall(resp, &authResp); err != nil {
			return "", ConstructNestedError("failed to unmarshal authentication response with error", err)
		}
		infolog.Printf("Authentication was successful")
		if tokenCache != nil {
			err = tokenCache.Put(getCacheKey(clientId, apiEndpoint), authResp.AccessToken, time.Duration(authResp.ExpiresIn)*time.Millisecond)
			if err != nil {
				infolog.Println(fmt.Errorf("failed to cache access token: %v", err))
			}
		}
		return authResp.AccessToken, nil
	}
}

// getCacheKey calculates an access token key using the username and the apiEndpoint provided
func getCacheKey(username, apiEndpoint string) string {
	return username + apiEndpoint
}

// getCachedAccessToken returns a cached access token or empty when a token could not be found
func getCachedAccessToken(username, apiEndpoint string) string {
	if tokenCache != nil {
		var cachedToken = tokenCache.Get(getCacheKey(username, apiEndpoint))
		if cachedToken != nil {
			return fmt.Sprint(cachedToken)
		}
	}
	return ""
}

// deleteAccessTokenFromCache deletes an access token from the cache if available
func deleteAccessTokenFromCache(username, apiEndpoint string) {
	if tokenCache != nil {
		err := tokenCache.Delete(getCacheKey(username, apiEndpoint))
		if err != nil {
			infolog.Println(fmt.Errorf("could not remove token from the memory cache: %v", err))
		}
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

// in the enpoint url, replase 'api' with 'id' in the beginning
func getAuthEndpoint(apiEndpoint string) (string, error) {
	u, err := url.Parse(apiEndpoint)
	if err != nil {
		return "", ConstructNestedError("error parsing api endpoint", err)
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
