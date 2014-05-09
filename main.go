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

type Conn struct {
	Hive          *tcliservice.TCLIServiceClient
	SessionHandle *tcliservice.TSessionHandle
}

type Stmt struct {
	Statement  *tcliservice.TExecuteStatementReq
	Connection *Conn
}

type Rows struct {
	Handle *tcliservice.TOperationHandle
}

type Driver struct{}

/*
Implementation method for driver.Driver.
*/

func (d *Driver) Open(host string) (driver.Conn, error) {
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
		return nil, fmt.Errorf("Error opening session: ", err)
	}

	return &Conn{client, session.SessionHandle}, nil
}

func (c *Conn) isOpen() bool {
	return c.SessionHandle != nil
}

/*
Implementation methods for driver.Conn.
*/

func (c *Conn) Prepare(query string) (driver.Stmt, error) {
	executeReq := tcliservice.NewTExecuteStatementReq()
	executeReq.SessionHandle = *c.SessionHandle
	executeReq.Statement = query

	return &Stmt{executeReq, c}, nil
}

func (c *Conn) Close() error {
	if c.isOpen() {
		closeReq := tcliservice.NewTCloseSessionReq()
		closeReq.SessionHandle = *c.SessionHandle
		resp, err := c.Hive.CloseSession(*closeReq)
		if err != nil {
			return fmt.Errorf("Error closing session: ", resp, err)
		}

		c.SessionHandle = nil
	}

	return nil
}

func (c *Conn) Begin() (driver.Tx, error) {
	// Hmm. Hive isn't transactional...
	return nil, nil
}

/*
Implementation methods for driver.Stmt.
*/

func (s *Stmt) Close() error {
	// No such thing.
	return nil
}

func (s *Stmt) NumInput() int {
	// TODO: Parse the query for :placeholder_parameters, count them,
	// return the total here.
	return -1
}

func (s *Stmt) Exec(args []driver.Value) (driver.Result, error) {
	// Let's just read-only for now.
	return nil, fmt.Errorf("INSERTs and UPDATEs are not supported!")
}

func (s *Stmt) Query(args []driver.Value) (driver.Rows, error) {
	log.Printf("query!")
	response, err := s.Connection.Hive.ExecuteStatement(*s.Statement)
	if err != nil {
		return nil, fmt.Errorf("Error in ExecuteStatement: ", response, err)
	}

	status := response.Status.GetStatusCode()
	if status != tcliservice.TStatusCode_SUCCESS_STATUS || status != tcliservice.TStatusCode_SUCCESS_WITH_INFO_STATUS {
		return nil, fmt.Errorf("Error from server: ", response.Status)
	}

	return &Rows{response.OperationHandle}, nil
}

/*
Implementation for driver.Rows.
*/

func (r *Rows) Columns() []string {
	return nil
}

func (r *Rows) Close() error {
	return nil
}

func (r *Rows) Next(dest []driver.Value) error {
	return io.EOF
}

func main() {
	sql.Register("hive", &Driver{})

	db, err := sql.Open("hive", "127.0.0.1:10000")
	if err != nil {
		panic(fmt.Errorf("Can't open database: ", err))
	}

	rows, err := db.Query("show tables")
	log.Println(rows, err)
	if err != nil {
		panic(fmt.Errorf("Error issuing query: ", err))
	}
}
