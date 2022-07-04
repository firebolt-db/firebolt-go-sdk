package fireboltgosdk

import (
	"encoding/json"
	"io/ioutil"
	"log"
	"net/http"
	"strings"
)

type Client struct {
	AccessToken string
}

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

	var c Client
	resp, err := c.Request("POST", LOGIN_URL, nil, string(jsonData))
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

func (c *Client) Request(method string, url string, params map[string]string, bodyStr string) ([]byte, error) {

	req, _ := http.NewRequest(method, url, strings.NewReader(bodyStr))
	req.Header.Add("Content-Type", "application/x-www-form-urlencoded")

	if len(c.AccessToken) > 0 {
		var bearer = "Bearer " + c.AccessToken
		req.Header.Add("Authorization", bearer)
	}

	q := req.URL.Query()
	for key, value := range params {
		q.Add(key, value)
	}
	req.URL.RawQuery = q.Encode()

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		log.Println(err)
		return nil, err
	}

	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Println(err)
		return nil, err
	}
	return body, nil
}
