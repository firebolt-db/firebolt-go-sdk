# Firebolt GO SDK

[![Nightly code check](https://github.com/yuryfirebolt/firebolt-go-sdk/actions/workflows/nightly.yml/badge.svg)](https://github.com/yuryfirebolt/firebolt-go-sdk/actions/workflows/nightly.yml)
[![Code quality checks](https://github.com/yuryfirebolt/firebolt-go-sdk/actions/workflows/code-check.yml/badge.svg)](https://github.com/yuryfirebolt/firebolt-go-sdk/actions/workflows/code-check.yml)
[![Integration tests](https://github.com/yuryfirebolt/firebolt-go-sdk/actions/workflows/integration-tests.yml/badge.svg)](https://github.com/yuryfirebolt/firebolt-go-sdk/actions/workflows/integration-tests.yml)
![Coverage](https://img.shields.io/endpoint?url=https://gist.githubusercontent.com/yuryfirebolt/764079ffbd558d515e250e6528179824/raw/firebolt-go-sdk-coverage.json)


Firebolt GO driver is an implementation of `database/sql/driver`.

### Installation

```shell
go get github.com/firebolt-db/firebolt-go-sdk
```

### Example
Here is an example of establishing a connection and executing a simple select query. 
For it to run successfully, you have to specify your credentials, and have a default engine up and running.
git checkout -b FIR-35471-remove-account-resolve-endpoint-from-go-sdk
```go
package main

import (
	"database/sql"
	"fmt"
	// we need to import firebolt-go-sdk, so it is able to register its driver
	_ "github.com/firebolt-db/firebolt-go-sdk"
)

func main() {

	// constructing a dsn string, you need to set your credentials
	clientId := ""
	clientSecret := ""
	accountName := ""
	databaseName := ""
	dsn := fmt.Sprintf("firebolt:///%s?account_name=%s&client_id=%s&client_secret=%s", databaseName, accountName, clientId, clientSecret)

	// opening the firebolt driver
	db, err := sql.Open("firebolt", dsn)
	if err != nil {
		fmt.Println("error during opening a driver: %v", err)
	}

	// executing a simple select query
	rows, err := db.Query("SELECT 1 UNION SELECT 2")
	if err != nil {
		fmt.Println("error during select query: %v", err)
	}

	// iterating over the resulting rows
	defer rows.Close()
	for rows.Next() {
		var id int
		if err := rows.Scan(&id); err != nil {
			fmt.Println("error during scan: %v", err)
		}
		fmt.Println(id)
	}
}
```


### DSN (Data source name)
All information for the connection should be specified using the DSN string. The firebolt dsn string has the following format:  
```
firebolt://[/database]?account_name=account_name&client_id=client_id&client_secret=client_secret[&engine=engine]
```

- **client_id** - credentials client id.
- **client_secret** - credentials client secret.
- **account_name** - the name of Firebolt account to log in to.
- **database** - (optional) the name of the database to connect to.
- **engine** - (optional) the name of the engine to run SQL on.

### Limitations
Although, all interfaces are available, not all of them are implemented or could be implemented:
- `driver.Result` is a dummy implementation and doesn't return the real result values.