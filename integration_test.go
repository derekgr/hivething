// +build integration

package hivething

import (
	"database/sql"
	"testing"
)

func TestConnection(t *testing.T) {
	var (
		tableName string
	)

	sql.Register("hive", NewDriver(DriverDefaults))
	db, err := sql.Open("hive", "127.0.0.1:10000")
	if err != nil {
		t.Fatalf("sql.Open error: %v", err)
	}

	rows, err := db.Query("SHOW TABLES")
	if err != nil {
		t.Fatalf("db.Query error: %v", err)
	}

	tables := 0
	for rows.Next() {
		err = rows.Scan(&tableName)
		if err != nil {
			t.Fatalf("rows.Scan error: %v", err)
		}
		tables += 1
	}

	if tables == 0 {
		t.Fatal("No tables retrieved!")
	}

	if tableName != "foo" {
		t.Errorf("Expected table 'foo' but was %s", tableName)
	}

	err = rows.Close()
	if err != nil {
		t.Errorf("rows.Close error: %v", err)
	}

	err = db.Close()
	if err != nil {
		t.Fatalf("db.Close error: %v", err)
	}
}
