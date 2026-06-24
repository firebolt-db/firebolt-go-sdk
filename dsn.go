package fireboltgosdk

import (
	"errors"
	"fmt"
	"net"
	"net/url"
	"regexp"
	"strings"
	"time"

	"github.com/firebolt-db/firebolt-go-sdk/types"

	"github.com/firebolt-db/firebolt-go-sdk/logging"
)

const dsnPattern = `^firebolt://(?:/(?P<database>\w+))?(?:\?(?P<parameters>[\w\.]+=[^=&]+(?:\&[\w\.]+=[^=&]+)*))?$`
const dsnPatternV0 = `^firebolt://(?P<username>.*@?.*):(?P<password>.*)@(?P<database>\w+)(?:/(?P<engine>[^?]+))?(?:\?(?P<parameters>[\w\.]+=[^=&]+(?:\&[\w\.]+=[^=&]+)*))?$`
const paramsPattern = `(?P<key>[\w\.]+)=(?P<value>[^=&]+)`
const sslModeStrict = "strict"
const sslModeNone = "none"

// ParseDSNString parses a dsn in a format: firebolt://username:password@db_name[/engine_name][?account_name=organization]
// returns a settings object where all parsed values are populated
// returns an error if required fields couldn't be parsed or if after parsing some characters were left unparsed
func ParseDSNString(dsn string) (*types.FireboltSettings, error) {
	dsnExpr := regexp.MustCompile(dsnPattern)
	dsnExprV0 := regexp.MustCompile(dsnPatternV0)

	logging.Infolog.Println("Parsing DSN")

	if dsnMatch := dsnExpr.FindStringSubmatch(dsn); len(dsnMatch) > 0 {
		return makeSettings(dsnMatch)
	} else if dsnMatch := dsnExprV0.FindStringSubmatch(dsn); len(dsnMatch) > 0 {
		return makeSettingsV0(dsnMatch)
	} else if settings, ok, err := makeDiscoverySettings(dsn); ok {
		return settings, err
	} else {
		return nil, errors.New("invalid connection string format")
	}
}

func makeSettings(dsnMatch []string) (*types.FireboltSettings, error) {
	var result types.FireboltSettings
	result.NewVersion = true
	result.ClientSideLB = true
	result.DefaultQueryParams = make(map[string]string)

	// Set database if it's provided
	if len(dsnMatch[1]) > 0 {
		result.Database = dsnMatch[1]
	}
	for _, m := range parseParams(dsnMatch[2]) {
		key := m[1]
		value := m[2]

		// Decode URL-encoded value
		decodedValue, err := url.QueryUnescape(value)
		if err != nil {
			return nil, fmt.Errorf("failed to URL decode parameter %s: %w", key, err)
		}

		// Check if this is a default_param.* prefixed parameter
		if strings.HasPrefix(key, "default_param.") {
			// Strip the prefix and add to DefaultQueryParams
			paramKey := strings.TrimPrefix(key, "default_param.")
			result.DefaultQueryParams[paramKey] = decodedValue
			continue
		}

		// Handle regular parameters
		switch key {
		case "account_name":
			result.AccountName = decodedValue
		case "engine":
			result.EngineName = decodedValue
		case "client_id":
			result.ClientID = decodedValue
		case "client_secret":
			result.ClientSecret = decodedValue
		case "url":
			result.Url = decodedValue
		case "ssl_mode":
			if decodedValue != sslModeStrict && decodedValue != sslModeNone {
				return nil, fmt.Errorf("invalid ssl_mode value %q", decodedValue)
			}
			result.SSLMode = decodedValue
		case "client_side_lb":
			result.ClientSideLB = decodedValue == "true"
		case "client_side_lb_dns_ttl":
			d, err := time.ParseDuration(decodedValue)
			if err != nil {
				return nil, fmt.Errorf("invalid client_side_lb_dns_ttl value %q: %w", decodedValue, err)
			}
			result.DNSTTL = d
		default:
			return nil, fmt.Errorf("unknown parameter name %s", key)
		}
	}
	return &result, nil
}

func makeDiscoverySettings(dsn string) (*types.FireboltSettings, bool, error) {
	parsed, err := url.Parse(dsn)
	if err != nil || parsed.Scheme != "firebolt" || parsed.Host == "" || parsed.User != nil {
		return nil, false, nil
	}

	result := types.FireboltSettings{
		NewVersion:           true,
		ClientSideLB:         true,
		DefaultQueryParams:   make(map[string]string),
		ConnectionParameters: make(map[string]string),
		SSLMode:              sslModeStrict,
	}

	query := parsed.Query()
	if pathDatabase := strings.TrimPrefix(parsed.EscapedPath(), "/"); pathDatabase != "" {
		database, err := url.PathUnescape(pathDatabase)
		if err != nil {
			return nil, true, fmt.Errorf("failed to URL decode database path: %w", err)
		}
		result.Database = database
		result.ConnectionParameters["database"] = database
	}

	for key, values := range query {
		if len(values) == 0 {
			continue
		}
		value := values[0]
		if strings.HasPrefix(key, "default_param.") {
			paramKey := strings.TrimPrefix(key, "default_param.")
			result.DefaultQueryParams[paramKey] = value
			continue
		}
		if err := applyDiscoveryParameter(&result, key, value); err != nil {
			return nil, true, err
		}
	}

	result.DiscoveryEndpoint = buildDiscoveryEndpoint(parsed, result.SSLMode)
	return &result, true, nil
}

func applyDiscoveryParameter(settings *types.FireboltSettings, key, value string) error {
	switch key {
	case "database":
		settings.Database = value
		settings.ConnectionParameters[key] = value
	case "engine":
		settings.EngineName = value
		settings.ConnectionParameters[key] = value
	case "ssl_mode":
		if value != sslModeStrict && value != sslModeNone {
			return fmt.Errorf("invalid ssl_mode value %q", value)
		}
		settings.SSLMode = value
	case "client_side_lb":
		settings.ClientSideLB = value == "true"
	case "client_side_lb_dns_ttl":
		d, err := time.ParseDuration(value)
		if err != nil {
			return fmt.Errorf("invalid client_side_lb_dns_ttl value %q: %w", value, err)
		}
		settings.DNSTTL = d
	case "url":
		return fmt.Errorf("url parameter is not supported for discovery DSNs")
	default:
		settings.ConnectionParameters[key] = value
	}
	return nil
}

func buildDiscoveryEndpoint(parsed *url.URL, sslMode string) string {
	scheme := "https"
	if sslMode == sslModeNone {
		scheme = "http"
	}
	host := parsed.Host
	if parsed.Port() != "" {
		host = net.JoinHostPort(parsed.Hostname(), parsed.Port())
	}
	return (&url.URL{Scheme: scheme, Host: host}).String()
}

func makeSettingsV0(dsnMatch []string) (*types.FireboltSettings, error) {
	var result types.FireboltSettings
	result.DefaultQueryParams = make(map[string]string)

	result.ClientID = dsnMatch[1]
	result.ClientSecret = dsnMatch[2]

	result.NewVersion = isServiceID(result.ClientID)

	result.Database = dsnMatch[3]
	if len(dsnMatch[4]) > 0 {
		// engine name was provided
		result.EngineName = dsnMatch[4]
	}

	for _, m := range parseParams(dsnMatch[5]) {
		key := m[1]
		value := m[2]

		// Decode URL-encoded value
		decodedValue, err := url.QueryUnescape(value)
		if err != nil {
			return nil, fmt.Errorf("failed to URL decode parameter %s: %w", key, err)
		}

		// Check if this is a default_param.* prefixed parameter
		if strings.HasPrefix(key, "default_param.") {
			// Strip the prefix and add to DefaultQueryParams
			paramKey := strings.TrimPrefix(key, "default_param.")
			result.DefaultQueryParams[paramKey] = decodedValue
			continue
		}

		// Handle regular parameters
		switch key {
		case "account_name":
			result.AccountName = decodedValue
		default:
			return nil, fmt.Errorf("unknown parameter name %s", key)
		}
	}

	return &result, nil
}

func parseParams(paramsStr string) [][]string {
	if len(paramsStr) == 0 {
		return make([][]string, 0)
	}
	paramsExpr := regexp.MustCompile(paramsPattern)
	return paramsExpr.FindAllStringSubmatch(paramsStr, -1)
}

func isServiceID(username string) bool {
	return !strings.Contains(username, "@")
}
