package fireboltgosdk

import (
	"encoding/json"
	"log"
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
	log.Printf("Start authentication into '%s' using '%s'", GetHostNameURL(), LoginUrl)

	values := map[string]string{"username": username, "password": password}
	jsonData, _ := json.Marshal(values)

	resp, err := request("", "POST", GetHostNameURL()+LoginUrl, nil, string(jsonData))
	if err != nil {
		return nil, ConstructNestedError("authentication request failed", err)
	}

	var authResp AuthenticationResponse
	err = jsonStrictUnmarshall(resp, &authResp)
	if err != nil {
		return nil, ConstructNestedError("failed to unmarshal authentication response with error", err)
	}

	log.Printf("Authentication was successful")
	return &Client{AccessToken: authResp.AccessToken}, nil
}
