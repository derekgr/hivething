Hivething is a small Go wrapper library around the [Hiveserver2](https://cwiki.apache.org/confluence/display/Hive/Setting+Up+HiveServer2) via its [Thrift interface](https://cwiki.apache.org/confluence/display/Hive/Setting+Up+HiveServer2).

## Usage

You can use this library in one of two ways: first, it implements the required interfaces for [database/sql/driver](http://golang.org/pkg/database/sql/driver/), and will interact syncronously with Hive (in other words,
blocking when necessary) through the SQL interface:

```go
import (
  "database/sql"
  "github.com/derekgr/hivething"
)

func Test() {
  sql.Register("hive", hivething.NewDriver())
  if conn, err := sql.Open("hive", "127.0.0.1:10000")
  if err != nil {
    // handle
  }


  // Use database connection with Query(), etc.
}
```

Alternatively, Hive supports asyncronous query execution, and so does this library. `hivething.HiveResultSet`, for example, exposes methods to poll for an asycronous query's status and results.
