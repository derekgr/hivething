package hivething

import (
	"database/sql/driver"
	"errors"
	"fmt"
	"github.com/derekgr/hivething/tcliservice"
	"io"
	//	"log"
	"time"
)

type Rows struct {
	thrift    *tcliservice.TCLIServiceClient
	operation *tcliservice.TOperationHandle

	columns []*tcliservice.TColumnDesc

	offset  int
	rowSet  *tcliservice.TRowSet
	hasMore bool
	ready   bool
}

type AsyncRows interface {
	Poll() (*Status, error)
	Wait() (*Status, error)
	WaitAndNotify(notify chan *Status)
}

type Status struct {
	state *tcliservice.TOperationState
	Error error
	At    time.Time
}

func newRows(thrift *tcliservice.TCLIServiceClient, operation *tcliservice.TOperationHandle) *Rows {
	return &Rows{thrift, operation, nil, 0, nil, true, false}
}

func (r *Rows) Poll() (*Status, error) {
	req := tcliservice.NewTGetOperationStatusReq()
	req.OperationHandle = *r.operation

	resp, err := r.thrift.GetOperationStatus(*req)
	if err != nil {
		return nil, fmt.Errorf("Error getting status: %+v, %v", resp, err)
	}

	if !isSuccessStatus(resp.Status) {
		return nil, fmt.Errorf("GetStatus call failed: %s", resp.Status.String())
	}

	return &Status{resp.OperationState, nil, time.Now()}, nil
}

func (r *Rows) Wait() (*Status, error) {
	for {
		status, err := r.Poll()

		if err != nil {
			return nil, err
		}

		if status.IsComplete() {
			if status.IsSuccess() {
				// Fetch operation metadata.
				metadataReq := tcliservice.NewTGetResultSetMetadataReq()
				metadataReq.OperationHandle = *r.operation

				metadataResp, err := r.thrift.GetResultSetMetadata(*metadataReq)
				if err != nil {
					return nil, err
				}

				if !isSuccessStatus(metadataResp.Status) {
					return nil, fmt.Errorf("GetResultSetMetadata failed: %s", metadataResp.Status.String())
				}

				r.columns = metadataResp.Schema.Columns
				r.ready = true

				return status, nil
			}
			return nil, fmt.Errorf("Query failed execution: %s", status.state.String())
		}

		time.Sleep(5)
	}
}

func (r *Rows) WaitAndNotify(notify chan *Status) {
	go func() {
		status, err := r.Wait()
		if status != nil {
			notify <- status
		} else if err != nil {
			notify <- &Status{nil, err, time.Now()}
		} else {
			notify <- &Status{nil, errors.New("unknown"), time.Now()}
		}
	}()
}

func (r *Rows) waitForSuccess() error {
	if !r.ready {
		status, err := r.Wait()
		if err != nil {
			return err
		}
		if !status.IsSuccess() || !r.ready {
			return fmt.Errorf("Unsuccessful query execution: %+v", status)
		}
	}

	return nil
}

func (r *Rows) Columns() []string {
	if err := r.waitForSuccess(); err != nil {
		return nil
	}

	ret := make([]string, len(r.columns))
	for i, col := range r.columns {
		ret[i] = col.ColumnName
	}

	return ret
}

func (r *Rows) Close() error {
	return nil
}

func (r *Rows) Next(dest []driver.Value) error {
	if err := r.waitForSuccess(); err != nil {
		return err
	}

	if r.rowSet == nil || r.offset >= len(r.rowSet.Rows) {
		if !r.hasMore {
			return io.EOF
		}

		fetchReq := tcliservice.NewTFetchResultsReq()
		fetchReq.OperationHandle = *r.operation
		fetchReq.Orientation = tcliservice.TFetchOrientation_FETCH_NEXT
		fetchReq.MaxRows = 10000

		resp, err := r.thrift.FetchResults(*fetchReq)
		if err != nil {
			return err
		}

		if !isSuccessStatus(resp.Status) {
			return fmt.Errorf("FetchResults failed: %s", resp.Status.String())
		}

		r.rowSet = resp.Results
		r.hasMore = *resp.HasMoreRows
	}

	row := r.rowSet.Rows[r.offset]
	if err := convertRow(row, dest); err != nil {
		return err
	}
	r.offset++

	return nil
}

func (r *Rows) FetchRow() ([]driver.Value, error) {
	row := make([]driver.Value, len(r.Columns()))
	err := r.Next(row)
	return row, err
}

func convertRow(row *tcliservice.TRow, dest []driver.Value) error {
	if len(row.ColVals) != len(dest) {
		return fmt.Errorf("Returned row has %d values, but scan row has %d", len(row.ColVals), len(dest))
	}

	for i, col := range row.ColVals {
		val, err := convertColumn(col)
		if err != nil {
			return fmt.Errorf("Error converting column %d: %v", i, err)
		}
		dest[i] = val
	}

	return nil
}

func convertColumn(col *tcliservice.TColumnValue) (driver.Value, error) {
	switch {
	case col.StringVal.IsSetValue():
		return col.StringVal.GetValue(), nil
	case col.BoolVal.IsSetValue():
		return driver.Bool.ConvertValue(col.BoolVal.GetValue())
	case col.ByteVal.IsSetValue():
		return int64(col.ByteVal.GetValue()), nil
	case col.I16Val.IsSetValue():
		return driver.Int32.ConvertValue(int32(col.I16Val.GetValue()))
	case col.I32Val.IsSetValue():
		return driver.Int32.ConvertValue(col.I32Val.GetValue())
	case col.I64Val.IsSetValue():
		return col.I64Val.GetValue(), nil
	case col.DoubleVal.IsSetValue():
		return col.DoubleVal.GetValue(), nil
	default:
		return nil, fmt.Errorf("Can't convert column value %v", col)
	}
}

func (s Status) String() string {
	if s.state == nil {
		return "unknown"
	}
	return s.state.String()
}

func (s Status) IsComplete() bool {
	switch *s.state {
	case tcliservice.TOperationState_FINISHED_STATE,
		tcliservice.TOperationState_CANCELED_STATE,
		tcliservice.TOperationState_CLOSED_STATE,
		tcliservice.TOperationState_ERROR_STATE:
		return true
	}
	return false
}

func (s Status) IsSuccess() bool {
	return *s.state == tcliservice.TOperationState_FINISHED_STATE
}
