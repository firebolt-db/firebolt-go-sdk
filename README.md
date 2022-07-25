# Firebolt GO SDK

[![Nightly code check](https://github.com/yuryfirebolt/firebolt-go-sdk/actions/workflows/nightly.yml/badge.svg)](https://github.com/yuryfirebolt/firebolt-go-sdk/actions/workflows/nightly.yml)
[![Code quality checks](https://github.com/yuryfirebolt/firebolt-go-sdk/actions/workflows/code-check.yml/badge.svg)](https://github.com/yuryfirebolt/firebolt-go-sdk/actions/workflows/code-check.yml)
[![Integration tests](https://github.com/yuryfirebolt/firebolt-go-sdk/actions/workflows/integration-tests.yml/badge.svg)](https://github.com/yuryfirebolt/firebolt-go-sdk/actions/workflows/integration-tests.yml)
![Coverage](https://img.shields.io/endpoint?url=https://gist.githubusercontent.com/yuryfirebolt/764079ffbd558d515e250e6528179824/raw/firebolt-go-sdk-coverage.json)


### Installation


```shell
go install github.com/firebolt-db/firebolt-go-sdk@latest
```

### Data source name
All information for the connection could be specifying the data source name.  
```
firebolt://username:password@db_name[/engine_name]?account_name=firebolt
```

It is possible to authenticate username and password. Additionally, database name has to be specified and the account      
Username, 


dsn string

### Example
code snippet
```go
```
