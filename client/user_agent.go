package client

import (
	"fmt"
	"os"
	"regexp"
	"runtime"
	"strings"

	"github.com/firebolt-db/firebolt-go-sdk/logging"
	"github.com/firebolt-db/firebolt-go-sdk/version"
	"github.com/matishsiao/goInfo"
)

var goInfoFunc = goInfo.GetInfo

// ConstructUserAgentString returns a string with go, GoSDK and os type and versions
// additionally user can set "FIREBOLT_GO_DRIVERS" and "FIREBOLT_GO_CLIENTS" env variable,
// and they will be concatenated with the final user-agent string
func ConstructUserAgentString() (ua_string string) {
	defer func() {
		// ConstructUserAgentString is a non-essential function, used for statistic gathering
		// so carry on working if a failure occurs
		if err := recover(); err != nil {
			logging.Infolog.Printf("Unable to generate User Agent string: %v", err)
			ua_string = "GoSDK"
		}
	}()
	osNameVersion := runtime.GOOS
	if gi, err := goInfoFunc(); err == nil {
		osNameVersion += " " + gi.Core
	}

	var isStringAllowed = regexp.MustCompile(`^[\w\d._\-/ ]+$`).MatchString

	goDrivers := os.Getenv("FIREBOLT_GO_DRIVERS")
	if !isStringAllowed(goDrivers) {
		goDrivers = ""
	}
	goClients := os.Getenv("FIREBOLT_GO_CLIENTS")
	if !isStringAllowed(goClients) {
		goClients = ""
	}

	ua_string = strings.TrimSpace(fmt.Sprintf("%s GoSDK/%s (Go %s; %s) %s", goClients, version.SdkVersion, runtime.Version(), osNameVersion, goDrivers))
	return ua_string
}
