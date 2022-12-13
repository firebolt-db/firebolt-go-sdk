package fireboltgosdk

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
)

var pool *sql.DB
var threadCount int

type atomicCounter int32

func (c *atomicCounter) inc() int32 {
	return atomic.AddInt32((*int32)(c), 1)
}

func (c *atomicCounter) get() int32 {
	return atomic.LoadInt32((*int32)(c))
}

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

	if err != nil {
		log.Fatal("error during opening a driver", err)
	}
	code := m.Run()
	pool.Close()
	os.Exit(code)
}

func BenchmarkSelectLineItemWithThreads(b *testing.B) {
	benchmarkSelectWithThreads(b, "select * from lineitem ORDER BY l_orderkey LIMIT 1000;")
}

func BenchmarkSelectLineItemWithoutThreads(b *testing.B) {
	benchmarkSelectWithoutThreads(b, "select * from lineitem ORDER BY l_orderkey LIMIT 1000;")
}

func BenchmarkSelect1WithThreads(b *testing.B) {
	benchmarkSelectWithThreads(b, "SELECT 1")
}

func BenchmarkSelect1WithoutThreads(b *testing.B) {
	benchmarkSelectWithoutThreads(b, "SELECT 1")
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
			b.Errorf("error during select query %s", err)
		}

		columns, err = rows.Columns()
		if err != nil {
			b.Errorf("error while getting columns %s", err)
		}
		columnCount := len(columns)
		values := make([]interface{}, columnCount)
		valuePointers := make([]interface{}, columnCount)
		for i, _ := range columns {
			valuePointers[i] = &values[i]
		}

		// iterating over the resulting rows
		for rows.Next() {
			if err := rows.Scan(valuePointers...); err != nil {
				b.Errorf("error during scan: %s", err)
			}
		}
		rows.Close()
	}

}
