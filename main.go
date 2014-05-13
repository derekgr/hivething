package main

import (
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"log"

	"./tcliservice"
	"git.apache.org/thrift.git/lib/go/thrift"
)

type conn struct {
	hive          *tcliservice.TCLIServiceClient
	sessionHandle *tcliservice.TSessionHandle
	options       *ConnOptions
}

type ConnOptions struct {
	QueryPagination int
}

type stmt struct {
	hive      *tcliservice.TCLIServiceClient
	statement *tcliservice.TExecuteStatementReq
}

type rows struct {
	hive   *tcliservice.TCLIServiceClient
	handle *tcliservice.TOperationHandle
	rowSet *tcliservice.TRowSet
}

type hivedriver struct {
	options *ConnOptions
}

func NewDriver(options *ConnOptions) driver.Driver {
	return &hivedriver{options}
}

func (d *hivedriver) Open(host string) (driver.Conn, error) {
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

	/*
		NB: hive 0.13's default is a TSaslProtocol, but
		there isn't a golang implementation in apache thrift as
		of this writing.
	*/
	protocol := thrift.NewTBinaryProtocolFactoryDefault()
	client := tcliservice.NewTCLIServiceClientFactory(transport, protocol)

	session, err := client.OpenSession(*tcliservice.NewTOpenSessionReq())
	if err != nil {
		return nil, err
	}

	return &conn{client, session.SessionHandle, d.options}, nil
}

func (c *conn) isOpen() bool {
	return c.sessionHandle != nil
}

/*
Implementation methods for driver.Conn.
*/

func (c *conn) Prepare(query string) (driver.Stmt, error) {
	executeReq := tcliservice.NewTExecuteStatementReq()
	executeReq.SessionHandle = *c.sessionHandle
	executeReq.Statement = query

	return &stmt{c.hive, executeReq}, nil
}

func (c *conn) Close() error {
	if c.isOpen() {
		closeReq := tcliservice.NewTCloseSessionReq()
		closeReq.SessionHandle = *c.sessionHandle
		resp, err := c.hive.CloseSession(*closeReq)
		if err != nil {
			return fmt.Errorf("Error closing session: ", resp, err)
		}

		c.sessionHandle = nil
	}

	return nil
}

func (c *conn) Begin() (driver.Tx, error) {
	// Hmm. Hive isn't transactional...
	return nil, nil
}

/*
Implementation methods for driver.Stmt.
*/

func (s *stmt) Close() error {
	// No such thing.
	return nil
}

func (s *stmt) NumInput() int {
	// TODO: Parse the query for :placeholder_parameters, count them,
	// return the total here.
	return -1
}

func (s *stmt) Exec(args []driver.Value) (driver.Result, error) {
	// Let's just read-only for now.
	return nil, fmt.Errorf("INSERTs and UPDATEs are not supported!")
}

func (s *stmt) Query(args []driver.Value) (driver.Rows, error) {
	response, err := s.hive.ExecuteStatement(*s.statement)
	if err != nil {
		return nil, fmt.Errorf("Error in ExecuteStatement: ", response, err)
	}

	status := response.Status.GetStatusCode()
	if status != tcliservice.TStatusCode_SUCCESS_STATUS && status != tcliservice.TStatusCode_SUCCESS_WITH_INFO_STATUS {
		return nil, fmt.Errorf("Error from server: ", response.Status)
	}

	return &rows{s.hive, response.OperationHandle, nil}, nil
}

/*
Implementation for driver.Rows.
*/

func (r *rows) Columns() []string {
	if r.rowSet == nil {
		r.Next(nil)
	}

	cols := make([]string, len(r.rowSet.Columns))
	for _, c := range r.rowSet.Columns {
		append(cols, c.String())
	}
	return cols
}

func (r *rows) Close() error {
	return nil
}

func (r *rows) Next(dest []driver.Value) error {
	return io.EOF
}

func main() {
	sql.Register("hive", NewDriver(&ConnOptions{QueryPagination: 1000}))

	db, err := sql.Open("hive", "127.0.0.1:10000")
	if err != nil {
		panic(fmt.Errorf("Can't open database: ", err))
	}

	rows, err := db.Query("show tables")
	log.Println("Query result: ", rows, err)
	if err != nil {
		panic(fmt.Errorf("Error issuing query: ", err))
	}
}
