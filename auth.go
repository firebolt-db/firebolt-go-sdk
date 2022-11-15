package fireboltgosdk

import (
	"context"
	"encoding/json"
	"net/url"
	"strings"
)

type AuthenticationResponse struct {
	AccessToken  string `json:"access_token"`
	RefreshToken string `json:"refresh_token"`
	ExpiresIn    int    `json:"expires_in"`
	TokenType    string `json:"token_type"`
	Scope        string `json:"scope"`
}

// Authenticate sends an authentication request, and returns a newly constructed client object
func Authenticate(username, password, apiEndpoint string) (*Client, error) {
	var loginUrl string
	var contentType string
	var body string
	var err error
	if strings.Contains(username, "@") {
		loginUrl, contentType, body, err = prepareUsernamePasswordLogin(username, password)
		if err != nil {
			return nil, err
		}
	} else {
		loginUrl, contentType, body = prepareServiceAccountLogin(username, password)
	}
	infolog.Printf("Start authentication into '%s' using '%s'", apiEndpoint, loginUrl)
	userAgent := ConstructUserAgentString()
	resp, err := request(context.TODO(), "", "POST", apiEndpoint+loginUrl, userAgent, nil, body, contentType)
	if err != nil {
		return nil, ConstructNestedError("authentication request failed", err)
	}

	var authResp AuthenticationResponse
	if err = jsonStrictUnmarshall(resp, &authResp); err != nil {
		return nil, ConstructNestedError("failed to unmarshal authentication response with error", err)
	}

	infolog.Printf("Authentication was successful")
	return &Client{AccessToken: authResp.AccessToken, ApiEndpoint: apiEndpoint, UserAgent: userAgent}, nil
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
