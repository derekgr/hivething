package hivething

import (
	"database/sql/driver"
	"fmt"
	"github.com/derekgr/hivething/tcliservice"
)

type Statement struct {
	thrift     *tcliservice.TCLIServiceClient
	executeReq *tcliservice.TExecuteStatementReq
}

func (s *Statement) Close() error {
	// No such thing.
	return nil
}

func (s *Statement) NumInput() int {
	// TODO: Parse the query for :placeholder_parameters, count them,
	// return the total here.
	return -1
}

func (s *Statement) Exec(args []driver.Value) (driver.Result, error) {
	// Let's just read-only for now.
	return nil, fmt.Errorf("INSERTs and UPDATEs are not supported!")
}

func (s *Statement) Query(args []driver.Value) (driver.Rows, error) {
	resp, err := s.thrift.ExecuteStatement(*s.executeReq)
	if err != nil {
		return nil, fmt.Errorf("Error in ExecuteStatement: %+v, %v", resp, err)
	}

	if !isSuccessStatus(resp.Status) {
		return nil, fmt.Errorf("Error from server: %s", resp.Status.String())
	}

	return NewRows(s.thrift, resp.OperationHandle), nil
}
