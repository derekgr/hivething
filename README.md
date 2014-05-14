Hivething is a small Go wrapper library around [Hiveserver2](https://cwiki.apache.org/confluence/display/Hive/Setting+Up+HiveServer2) via its [Thrift interface](https://cwiki.apache.org/confluence/display/Hive/Setting+Up+HiveServer2).

## Usage

```go
import (
  "github.com/derekgr/hivething"
)

func ListTablesAsync() []string {
  db, err := hivething.Connect("127.0.0.1:10000", hivething.DefaultOptions)
  if err != nil {
    // handle
  }
  defer db.Close()

  results, err := db.Query("SHOW TABLES")
  if err != nil {
      // handle
  }

  status, err := results.Wait()
  if err != nil {
      // handle
  }

  if status.IsSuccess() {
      var tableName string
      for results.Next() {
          results.Scan(&tableName)
          append(tables, tableName)
      }
  }
  else {
      // handle status.Error
  }

  return tables
}
```
