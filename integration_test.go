// +build integration

package hivething

import (
	"database/sql"
	"testing"
)

func TestConnection(t *testing.T) {
	sql.Register("hive", NewDriver(DriverDefaults))
	db, err := sql.Open("hive", "127.0.0.1:10000")
	if err != nil {
		t.Errorf("sql.Open error: %v", err)
	}

	if db == nil {
		t.Error("sql.Open returned a nil db")
	}

	rows, err := db.Query("SHOW TABLES")
	if err != nil {
		t.Errorf("db.Query error: %v", err)
	}

	if rows == nil {
		t.Error("db.Query returned nil rows")
	}

	t.Logf("%+v", rows)
}
