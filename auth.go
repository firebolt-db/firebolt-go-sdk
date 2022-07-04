package fireboltgosdk

import (
	"encoding/json"
	"log"
)

// Authenticate sends an authentication request, and returns a newly constructed client object
func Authenticate(username, password string) (*Client, error) {
	// AuthenticationResponse definition of the authentication response
	type AuthenticationResponse struct {
		AccessToken string `json:"access_token"`
		ExpiresIn   int    `json:"expires_in"`
		TokenType   string `json:"token_type"`
		Scope       string `json:"scope"`
	}

	values := map[string]string{"username": username, "password": password}
	jsonData, _ := json.Marshal(values)

	var c Client
	resp, err := c.Request("POST", LoginUrl, nil, string(jsonData))
	if err != nil {
		log.Fatal(err)
		return nil, err
	}

	var authResp AuthenticationResponse
	err = json.Unmarshal(resp, &authResp)
	if err != nil {
		log.Fatal(err)
		return nil, err
	}
	c.AccessToken = authResp.AccessToken

	return &c, nil
}
