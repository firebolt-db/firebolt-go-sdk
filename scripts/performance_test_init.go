package main

import (
	"database/sql"
	"fmt"
	_ "github.com/firebolt-db/firebolt-go-sdk"
	"log"
	"os"
)

func main() {
	username := os.Getenv("USER_NAME")
	password := os.Getenv("PASSWORD")
	databaseName := os.Getenv("DATABASE_NAME")
	dsn := fmt.Sprintf("firebolt://%s:%s@%s", username, password, databaseName)

	db, err := sql.Open("firebolt", dsn)

	if err != nil {
		log.Fatal("error while opening a driver", err)
	}
	queries := []string{
		"CREATE EXTERNAL TABLE IF NOT EXISTS ex_lineitem ( l_orderkey LONG, l_partkey LONG, l_suppkey LONG, l_linenumber INT, l_quantity LONG, l_extendedprice LONG, l_discount LONG, l_tax LONG, l_returnflag TEXT, l_linestatus TEXT, l_shipdate TEXT, l_commitdate TEXT, l_receiptdate TEXT, l_shipinstruct TEXT, l_shipmode TEXT, l_comment TEXT)URL = 's3://firebolt-publishing-public/samples/tpc-h/parquet/lineitem/'OBJECT_PATTERN = '*.parquet'TYPE = (PARQUET)",
		"CREATE FACT TABLE IF NOT EXISTS lineitem ( l_orderkey LONG, l_partkey LONG, l_suppkey LONG, l_linenumber INT, l_quantity LONG, l_extendedprice LONG, l_discount LONG, l_tax LONG, l_returnflag TEXT, l_linestatus TEXT, l_shipdate TEXT, l_commitdate TEXT, l_receiptdate TEXT, l_shipinstruct TEXT, l_shipmode TEXT, l_comment TEXT ) PRIMARY INDEX l_orderkey, l_linenumber",
		"INSERT INTO lineitem SELECT * FROM ex_lineitem"}

	for _, query := range queries {
		_, err := db.Query(query)
		if err != nil {
			log.Fatalf("the query %s returned an error: %v", query, err)
		}
	}
	db.Close()
}
