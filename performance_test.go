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

func BenchmarkSelectWithThreads(b *testing.B) {
	var counter atomicCounter
	var total = int32(b.N)
	var wg sync.WaitGroup
	wg.Add(threadCount)
	for j := 0; j < threadCount; j++ {
		go func(i int) {
			for counter.get() < total {
				counter.inc()
				executeSelect(1, b)
			}
			defer wg.Done()
		}(j)
	}
	wg.Wait()
}

func BenchmarkSelectWithoutThreads(b *testing.B) {
	executeSelect(b.N, b)
}

func executeSelect(count int, b *testing.B) {
	for i := 0; i < count; i++ {
		rows, err := pool.Query("select * from lineitem ORDER BY l_orderkey LIMIT 1000;")

		if err != nil {
			b.Errorf("error during select query %s", err)
		}
		var anyField any
		// iterating over the resulting rows
		defer rows.Close()
		for rows.Next() {
			if err := rows.Scan(&anyField, &anyField, &anyField, &anyField, &anyField, &anyField, &anyField, &anyField, &anyField, &anyField, &anyField, &anyField, &anyField, &anyField, &anyField, &anyField); err != nil {
				b.Errorf("error during scan: %s", err)
			}
		}
	}

}
