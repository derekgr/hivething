package hivething

import (
	"database/sql/driver"
	"fmt"
	"github.com/derekgr/hivething/tcliservice"
)

type Connection struct {
	thrift  *tcliservice.TCLIServiceClient
	session *tcliservice.TSessionHandle
	options Options
}

func (c *Connection) isOpen() bool {
	return c.session != nil
}

func (c *Connection) Prepare(query string) (driver.Stmt, error) {
	executeReq := tcliservice.NewTExecuteStatementReq()
	executeReq.SessionHandle = *c.session
	executeReq.Statement = query

	return &Statement{c.thrift, executeReq}, nil
}

func (c *Connection) Close() error {
	if c.isOpen() {
		closeReq := tcliservice.NewTCloseSessionReq()
		closeReq.SessionHandle = *c.session
		resp, err := c.thrift.CloseSession(*closeReq)
		if err != nil {
			return fmt.Errorf("Error closing session: ", resp, err)
		}

		c.session = nil
	}

	return nil
}

func (c *Connection) Begin() (driver.Tx, error) {
	// No support for transactions...
	return nil, nil
}

func (c *Connection) QueryAsync(query string, args ...driver.Value) (*Rows, error) {
	executeReq := tcliservice.NewTExecuteStatementReq()
	executeReq.SessionHandle = *c.session
	executeReq.Statement = query

	statement := &Statement{c.thrift, executeReq}
	return statement.executeQuery(args)
}
