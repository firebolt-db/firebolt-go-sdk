package fireboltgosdk

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"strconv"
	"sync"
	"testing"
)

var pool *sql.DB
var threadCount int
var selectLineItemQuery = "select * from lineitem ORDER BY l_orderkey LIMIT 1000;"
var select1Query = "select 1;"

func TestMain(m *testing.M) {
	// constructing a dsn string, you need to set your credentials
	username := os.Getenv("USER_NAME")
	password := os.Getenv("PASSWORD")
	databaseName := os.Getenv("DATABASE_NAME")
	threadCount, _ = strconv.Atoi(os.Getenv("TEST_THREAD_COUNT"))
	dsn := fmt.Sprintf("firebolt://%s:%s@%s", username, password, databaseName)
	var err error
	// creating the connection pool
	pool, err = sql.Open("firebolt", dsn)
	defer pool.Close()

	if err != nil {
		log.Fatal("error during opening a driver", err)
	}
	code := m.Run()
	os.Exit(code)
}

func BenchmarkSelectLineItemWithThreads(b *testing.B) {
	benchmarkSelectWithThreads(b, selectLineItemQuery)
}

func BenchmarkSelectLineItemWithoutThreads(b *testing.B) {
	benchmarkSelectWithoutThreads(b, selectLineItemQuery)
}

func BenchmarkSelect1WithThreads(b *testing.B) {
	benchmarkSelectWithThreads(b, select1Query)
}

func BenchmarkSelect1WithoutThreads(b *testing.B) {
	benchmarkSelectWithoutThreads(b, select1Query)
}

func benchmarkSelectWithThreads(b *testing.B, query string) {
	var loops = b.N
	var wg sync.WaitGroup
	wg.Add(threadCount)
	for j := 0; j < threadCount; j++ {
		go func(i int) {
			executeQuery(loops, query, b)
			defer wg.Done()
		}(j)
	}
	wg.Wait()
}

func benchmarkSelectWithoutThreads(b *testing.B, query string) {
	executeQuery(b.N, query, b)
}

func executeQuery(loops int, query string, b *testing.B) {
	var columns []string
	for i := 0; i < loops; i++ {
		rows, err := pool.Query(query)
		if err != nil {
			b.Errorf("error during select query %v", err)
		}

		//Because the function is used for different queries, we only know the number of columns at runtime.
		columns, err = rows.Columns()
		if err != nil {
			b.Errorf("error while getting columns %v", err)
		}
		columnCount := len(columns)
		values := make([]interface{}, columnCount)
		valuePointers := make([]interface{}, columnCount)
		for i := range columns {
			valuePointers[i] = &values[i]
		}

		// iterating over the resulting rows
		for rows.Next() {
			if err := rows.Scan(valuePointers...); err != nil {
				b.Errorf("error during scan: %v", err)
			}
		}
		err = rows.Close()
		if err != nil {
			b.Errorf("error while closing the row %v", err)
		}
	}

}
