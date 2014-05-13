Hivething is a small Go wrapper library around [Hiveserver2](https://cwiki.apache.org/confluence/display/Hive/Setting+Up+HiveServer2) via its [Thrift interface](https://cwiki.apache.org/confluence/display/Hive/Setting+Up+HiveServer2).

## Usage

You can use this library in one of two ways: first, it implements the required interfaces for [database/sql/driver](http://golang.org/pkg/database/sql/driver/), and will interact syncronously with Hive (in other words,
blocking when necessary) through the SQL interface:

```go
import (
  "database/sql"
  "github.com/derekgr/hivething"
)

func ListTables() []string {
  sql.Register("hive", hivething.DefaultDriver)
  if db, err := sql.Open("hive", "127.0.0.1:10000")
  if err != nil {
    // handle
  }

  rows, err := db.Query("SHOW TABLES")
  if err != nil {
      // handle
  }


  tables := make([]string)
  for rows.Next() {
      var tableName string
      rows.Scan(&tableName)
      append(tables, tableName)
  }

  return tables
}
```

Alternatively, Hive supports asyncronous query execution, which you can use via the `Poll()` and `Wait()` methods from a `hivething.Rows`. There's
also `WaitAndNotify(chan hivething.Status)`, which will poll in a goroutine until the job's complete (one way or another), and then notify you on
a notification channel.

```go
import (
  "github.com/derekgr/hivething"
)

func ListTablesAsync() []string {
  if db, err := hivething.DefaultDriver.OpenConnection("127.0.0.1:10000")
  if err != nil {
    // handle
  }

  rows, err := db.QueryAsync("SHOW TABLES")
  if err != nil {
      // handle
  }

  notify := make(chan *hivething.Status, 1)

  // Blocks until operation complete, but you could call Poll() to get
  // incremental status updates without waiting.
  rows.WaitAndNotify(notify)

  status := <-notify
  tables := make([]string)
  if status.IsSuccess() {
      for {
          row, err := rows.NextRow()
          if err == io.EOF {
              // No more data
              break
          }

          if err != nil {
              // handle err
          }

          append(tables, row[0].(string))
      }
  }
  else {
      // handle status.Error
  }

  return tables
}
```
