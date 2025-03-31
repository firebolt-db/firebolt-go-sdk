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

### DSN (Data source name)
All information for the connection should be specified using the DSN string. The firebolt dsn string has the following format:
```
firebolt://[/database]?account_name=account_name&client_id=client_id&client_secret=client_secret[&engine=engine]
```

- **client_id** - client id of the [service account](https://docs.firebolt.io/sql_reference/information-schema/service-accounts.html).
- **client_secret** - client secret of the [service account](https://docs.firebolt.io/sql_reference/information-schema/service-accounts.html).
- **account_name** - the name of Firebolt account to log in to.
- **database** - (optional) the name of the database to connect to.
- **engine** - (optional) the name of the engine to run SQL on.

### Querying example
Here is an example of establishing a connection and executing a simple select query.
For it to run successfully, you have to specify your credentials, and have a default engine up and running.

```go
package main

import (
	"database/sql"
	"fmt"
	"log"

	// we need to import firebolt-go-sdk in order to register the driver
	_ "github.com/firebolt-db/firebolt-go-sdk"
)

func main() {

	// set your Firebolt credentials to construct a dsn string
	clientId := ""
	clientSecret := ""
	accountName := ""
	databaseName := ""
	engineName := ""
	dsn := fmt.Sprintf("firebolt:///%s?account_name=%s&client_id=%s&client_secret=%s&engine=%s", databaseName, accountName, clientId, clientSecret, engineName)

	// open a Firebolt connection
	db, err := sql.Open("firebolt", dsn)
	if err != nil {
		log.Fatalf("error during opening a driver: %v", err)
	}

	// create a table
	_, err = db.Query("CREATE TABLE test_table(id INT, value TEXT)")
	if err != nil {
		log.Fatalf("error during select query: %v", err)
	}

	// execute a parametrized insert (only ? placeholders are supported)
	_, err = db.Query("INSERT INTO test_table VALUES (?, ?)", 1, "my value")
	if err != nil {
		log.Fatalf("error during select query: %v", err)
	}

	// execute a simple select query
	rows, err := db.Query("SELECT id FROM test_table")
	if err != nil {
		log.Fatalf("error during select query: %v", err)
	}

	// iterate over the result
	defer func() {
		if err := rows.Close(); err != nil {
			log.Printf("error during rows.Close(): %v\n", err)
		}
	}()

	for rows.Next() {
		var id int
		if err := rows.Scan(&id); err != nil {
			log.Fatalf("error during scan: %v", err)
		}
		log.Print(id)
	}

	if err := rows.Err(); err != nil {
		log.Fatalf("error during rows iteration: %v\n", err)
	}
}
```

### Streaming example
In order to stream the query result (and not store it in memory fully), you need to pass a special context with streaming enabled.
> **Warning**: If you enable streaming the result, the query execution might finish successfully, but the actual error might be returned during the iteration over the rows.   

Here is an example of how to do it:

```go
package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"

	// we need to import firebolt-go-sdk in order to register the driver
	_ "github.com/firebolt-db/firebolt-go-sdk"
	fireboltContext "github.com/firebolt-db/firebolt-go-sdk/context"
)

func main() {
	// set your Firebolt credentials to construct a dsn string
	clientId := ""
	clientSecret := ""
	accountName := ""
	databaseName := ""
	engineName := ""
	dsn := fmt.Sprintf("firebolt:///%s?account_name=%s&client_id=%s&client_secret=%s&engine=%s", databaseName, accountName, clientId, clientSecret, engineName)

	// open a Firebolt connection
	db, err := sql.Open("firebolt", dsn)
	if err != nil {
		log.Fatalf("error during opening a driver: %v", err)
	}

	// create a streaming context
	streamingCtx := fireboltContext.WithStreaming(context.Background())

	// execute a large select query
	rows, err := db.QueryContext(streamingCtx, "SELECT 'abc' FROM generate_series(1, 100000000)")
	if err != nil {
		log.Fatalf("error during select query: %v", err)
	}

	// iterating over the result is exactly the same as in the previous example
	defer func() {
		if err := rows.Close(); err != nil {
			log.Printf("error during rows.Close(): %v\n", err)
		}
	}()

	for rows.Next() {
		var id int
		if err := rows.Scan(&id); err != nil {
			log.Fatalf("error during scan: %v", err)
		}
		log.Print(id)
	}

	if err := rows.Err(); err != nil {
		log.Fatalf("error during rows iteration: %v\n", err)
	}
}
```

#### Errors in streaming
If you enable streaming the result, the query execution might finish successfully, but the actual error might be returned during the iteration over the rows.


### Limitations
Although, all interfaces are available, not all of them are implemented or could be implemented:
- `driver.Result` is a dummy implementation and doesn't return the real result values.
- Named query parameters are not supported
