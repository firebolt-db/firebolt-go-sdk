package fireboltgosdk

import (
	"encoding/json"
	"fmt"
)

// AuthenticationResponse definition of the authentication response
type AuthenticationResponse struct {
	AccessToken  string `json:"access_token"`
	RefreshToken string `json:"refresh_token"`
	ExpiresIn    int    `json:"expires_in"`
	TokenType    string `json:"token_type"`
	Scope        string `json:"scope"`
}

// Authenticate sends an authentication request, and returns a newly constructed client object
func Authenticate(username, password string) (*Client, error) {

	values := map[string]string{"username": username, "password": password}
	jsonData, _ := json.Marshal(values)

	resp, err := request("", "POST", HostNameURL+LoginUrl, nil, string(jsonData))
	if err != nil {
		return nil, fmt.Errorf("authentication request failed: %v", err)
	}

	var authResp AuthenticationResponse
	err = jsonStrictUnmarshall(resp, &authResp)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarhal authenication response: %s", resp)
	}

	return &Client{AccessToken: authResp.AccessToken}, nil
}
