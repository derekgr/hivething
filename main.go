package hivething

import (
	"errors"
	"fmt"
	"log"

	"git.apache.org/thrift.git/lib/go/thrift"
	thrifthive "github.com/derekgr/hivething/TCLIService"
)

type Connection struct {
	Hive *thrifthive.TCLIServiceClient
}

func Connect(host string) (*Connection, error) {
	transport, err := thrift.NewTSocket(host)
	if err != nil {
		return nil, err
	}

	if err := transport.Open(); err != nil {
		return nil, err
	}

	if transport == nil {
		return nil, errors.New("nil thrift transport")
	}

	protocol := thrift.NewTBinaryProtocolFactoryDefault()
	client := thrifthive.NewTCLIServiceClientFactory(transport, protocol)

	return &Connection{client}, nil
}

// Let's just try a simple "open a connection, execute a statement test".
func (c *Connection) Query(query string) error {
	resp, err := c.Hive.OpenSession(thrifthive.NewTOpenSessionReq())
	if err != nil {
		return fmt.Errorf("Error opening session: %v", err)
	}

	log.Println(resp)

	// I think I need to set a statement handle here,
	// statement.Handle, from the OpenSession above, but
	// I can't get it to work.
	statement := thrifthive.NewTExecuteStatementReq()
	statement.Statement = query
	q, err := c.Hive.ExecuteStatement(statement)
	if err != nil {
		return fmt.Errorf("Error in ExecuteStatement: %v", err)
	}

	log.Println(q)
	return nil
}
