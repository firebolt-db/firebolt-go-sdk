package fireboltgosdk

import (
	"encoding/json"
)

type AuthenticationResponse struct {
	AccessToken  string `json:"access_token"`
	RefreshToken string `json:"refresh_token"`
	ExpiresIn    int    `json:"expires_in"`
	TokenType    string `json:"token_type"`
	Scope        string `json:"scope"`
}

// Authenticate sends an authentication request, and returns a newly constructed client object
func Authenticate(username, password string) (*Client, error) {
	infolog.Printf("Start authentication into '%s' using '%s'", HostNameURL, LoginUrl)

	values := map[string]string{"username": username, "password": password}
	jsonData, _ := json.Marshal(values)

	resp, err := request("", "POST", HostNameURL+LoginUrl, nil, string(jsonData))
	if err != nil {
		return nil, ConstructNestedError("authentication request failed", err)
	}

	var authResp AuthenticationResponse
	err = jsonStrictUnmarshall(resp, &authResp)
	if err != nil {
		return nil, ConstructNestedError("failed to unmarshal authentication response with error", err)
	}

	infolog.Printf("Authentication was successful")
	return &Client{AccessToken: authResp.AccessToken}, nil
}
