package client

import (
	"encoding/json"
	"net/url"
	"testing"
)

func TestPrepareServiceAccountLoginIsFormEncoded(t *testing.T) {
	loginURL, contentType, body := prepareServiceAccountLogin("id", "secret&=", "aud")

	if loginURL != ServiceAccountLoginURLSuffix {
		t.Errorf("expected login URL %q, got %q", ServiceAccountLoginURLSuffix, loginURL)
	}
	if contentType != ContentTypeForm {
		t.Errorf("expected Content-Type %q, got %q", ContentTypeForm, contentType)
	}
	form, err := url.ParseQuery(body)
	if err != nil {
		t.Fatalf("body is not form-encoded: %v", err)
	}
	expected := map[string]string{"client_id": "id", "client_secret": "secret&=", "grant_type": "client_credentials", "audience": "aud"}
	for key, value := range expected {
		if got := form.Get(key); got != value {
			t.Errorf("expected %s=%q, got %q", key, value, got)
		}
	}
}

func TestPrepareUsernamePasswordLoginIsJSON(t *testing.T) {
	loginURL, contentType, body, err := prepareUsernamePasswordLogin("user", `pa"ss`)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if loginURL != UsernamePasswordURLSuffix {
		t.Errorf("expected login URL %q, got %q", UsernamePasswordURLSuffix, loginURL)
	}
	if contentType != ContentTypeJSON {
		t.Errorf("expected Content-Type %q, got %q", ContentTypeJSON, contentType)
	}
	var payload map[string]string
	if err := json.Unmarshal([]byte(body), &payload); err != nil {
		t.Fatalf("body is not JSON: %v", err)
	}
	if payload["username"] != "user" || payload["password"] != `pa"ss` {
		t.Errorf("unexpected payload %v", payload)
	}
}
