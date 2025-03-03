package client

import (
	"os"
	"strings"
	"testing"

	"github.com/firebolt-db/firebolt-go-sdk/version"
	"github.com/matishsiao/goInfo"
)

func TestConstructUserAgentString(t *testing.T) {
	os.Setenv("FIREBOLT_GO_DRIVERS", "GORM/0.0.1")
	os.Setenv("FIREBOLT_GO_CLIENTS", "Client1/0.2.3 Client2/0.3.4")

	defer func() {
		os.Unsetenv("FIREBOLT_GO_DRIVERS")
		os.Unsetenv("FIREBOLT_GO_CLIENTS")
	}()

	userAgentString := ConstructUserAgentString()

	if !strings.Contains(userAgentString, version.SdkVersion) {
		t.Errorf("sdk Version is not in userAgent string")
	}
	if !strings.Contains(userAgentString, "GoSDK") {
		t.Errorf("sdk name is not in userAgent string")
	}
	if !strings.Contains(userAgentString, "GORM/0.0.1") {
		t.Errorf("drivers is not in userAgent string")
	}
	if !strings.Contains(userAgentString, "Client1/0.2.3 Client2/0.3.4") {
		t.Errorf("clients are not in userAgent string")
	}
}

// FIR-25705
func TestConstructUserAgentStringFails(t *testing.T) {
	// Save current function and restore at the end
	old := goInfoFunc
	defer func() { goInfoFunc = old }()

	goInfoFunc = func() (goInfo.GoInfoObject, error) {
		// Simulate goinfo failing
		panic("Aaaaaaaaaa")
	}
	userAgentString := ConstructUserAgentString()

	if userAgentString != "GoSDK" {
		t.Errorf("UserAgent string was not generated correctly")
	}
}
