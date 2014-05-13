// +build integration

package hivething

import (
	"database/sql"
	"io"
	"testing"
)

/*
Used with local testing: expects hiveserver2 running on 127.0.0.1:10000,
with a single table defined, "foo".
*/

func TestSQLInterface(t *testing.T) {
	var (
		tableName string
	)

	db, err := sql.Open("hive", "127.0.0.1:10000")
	if err != nil {
		t.Fatalf("sql.Open error: %v", err)
	}

	rows, err := db.Query("SHOW TABLES")
	if err != nil {
		t.Fatalf("db.Query error: %v", err)
	}

	var ct int = 0
	for rows.Next() {
		if ct > 0 {
			t.Fatal("Expected a single row to be fetched")
		}

		err = rows.Scan(&tableName)
		if err != nil {
			t.Fatalf("rows.Scan error: %v", err)
		}
	}

	if tableName != "foo" {
		t.Errorf("Expected table 'foo' but found %s", tableName)
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

func TestAsyncInterface(t *testing.T) {
	var (
		tableName string
	)
	conn, err := DefaultDriver.OpenConnection("127.0.0.1:10000")
	if err != nil {
		t.Fatalf("Driver.OpenConnection error %v", err)
	}

	rows, err := conn.QueryAsync("SHOW TABLES")
	if err != nil {
		t.Fatalf("Connection.QueryAsync error: %v", err)
	}

	notify := make(chan *Status, 1)
	rows.WaitAndNotify(notify)

	status := <-notify
	if !status.IsSuccess() {
		t.Fatalf("Unsuccessful query execution: %+v", status)
	}

	var ct int = 0
	for {
		if ct > 1 {
			t.Fatal("Rows.FetchRow should terminate after 1 fetch")
		}

		row, err := rows.FetchRow()
		if err == io.EOF {
			break
		}

		if err != nil {
			t.Fatalf("Rows.NextRow error: %v", err)
		}

		tableName = row[0].(string)
		ct++
	}

	if tableName != "foo" {
		t.Errorf("Expected table 'foo' but found %s", tableName)
	}
}

func TestSQLQuery(t *testing.T) {
	var (
		id    int
		value string
	)

	db, err := sql.Open("hive", "127.0.0.1:10000")
	rows, err := db.Query("select * from foo")
	if err != nil {
		t.Fatalf("db.Query error: %v", err)
	}

	col, err := rows.Columns()
	t.Logf("%+v\n", col)
	for rows.Next() {
		rows.Scan(&id, &value)
		t.Logf("%d\t%s", id, value)
	}
}

func TestAsyncQuery(t *testing.T) {
	var (
		id    int64
		value string
	)

	db, err := DefaultDriver.OpenConnection("127.0.0.1:10000")
	rows, err := db.QueryAsync("select * from foo")
	if err != nil {
		t.Fatalf("db.Query error: %v", err)
	}

	status, err := rows.Wait()
	if status.IsSuccess() {
		col := rows.Columns()
		t.Logf("%+v\n", col)
		for {
			row, err := rows.FetchRow()
			if err == io.EOF {
				break
			}
			id = row[0].(int64)
			value = row[1].(string)
			t.Logf("%d\t%s", id, value)
		}
	}
}

func init() {
	sql.Register("hive", DefaultDriver)
}
