package fireboltgosdk

import (
	"encoding/json"
	"fmt"
	"log"
)

// AuthenticationResponse definition of the authentication response
type AuthenticationResponse struct {
	AccessToken string `json:"access_token"`
	ExpiresIn   int    `json:"expires_in"`
	TokenType   string `json:"token_type"`
	Scope       string `json:"scope"`
}

// Authenticate sends an authentication request, and returns a newly constructed client object
func Authenticate(username, password string) (*Client, error) {

	values := map[string]string{"username": username, "password": password}
	jsonData, _ := json.Marshal(values)

	resp, err := request("", "POST", HostNameURL+LoginUrl, nil, string(jsonData))
	if err != nil {
		log.Fatal(err)
		return nil, fmt.Errorf("authentication request failed: %v", err)
	}

	var authResp AuthenticationResponse
	err = json.Unmarshal(resp, &authResp)
	if err != nil {
		log.Fatal(err)
		return nil, fmt.Errorf("failed to unmarhal authenication response: %v", err)
	}

	return &Client{AccessToken: authResp.AccessToken}, nil
}
