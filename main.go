package main

import (
	"errors"
	"fmt"
	"log"

	"git.apache.org/thrift.git/lib/go/thrift"
	"./tcliservice"
)

type Connection struct {
	Hive *tcliservice.TCLIServiceClient
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
	client := tcliservice.NewTCLIServiceClientFactory(transport, protocol)

	return &Connection{client}, nil
}

// Let's just try a simple "open a connection, execute a statement test".
func (c *Connection) Query(query string) error {
	session, err := c.Hive.OpenSession(*tcliservice.NewTOpenSessionReq())
	if err != nil {
		return fmt.Errorf("Error opening session: %v", err)
	}

	executeReq := tcliservice.NewTExecuteStatementReq()
	executeReq.SessionHandle = *session.SessionHandle
	executeReq.Statement = query
	execute, err := c.Hive.ExecuteStatement(*executeReq)
	if err != nil {
		return fmt.Errorf("Error in ExecuteStatement: %v", err)
	}

	fetchReq := tcliservice.NewTFetchResultsReq()
	fetchReq.OperationHandle = *execute.OperationHandle
	fetchReq.MaxRows = 128
	fetch, err := c.Hive.FetchResults(*fetchReq)
	if err != nil {
		return err
	}

	log.Println(fetch)

	return nil
}

func main() {
	conn, err := Connect("127.0.0.1:10000")
	if err != nil {
		panic(err)
	}

	err = conn.Query("show tables")
	if err != nil {
		panic(err)
	}
}
