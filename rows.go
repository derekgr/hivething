package hivething

import (
	"database/sql/driver"
	"fmt"
	"github.com/derekgr/hivething/tcliservice"
	"io"
	"time"
)

type Rows struct {
	thrift    *tcliservice.TCLIServiceClient
	operation *tcliservice.TOperationHandle

	columns []*tcliservice.TColumnDesc

	offset  int
	rowSet  *tcliservice.TRowSet
	hasMore bool
}

type AsyncRows interface {
	Poll() (*Status, error)
	Wait() (*Status, error)
}

type Status struct {
	state *tcliservice.TOperationState
	At    time.Time
}

func NewRows(thrift *tcliservice.TCLIServiceClient, operation *tcliservice.TOperationHandle) *Rows {
	return &Rows{thrift, operation, nil, 0, nil, true}
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

	return &Status{resp.OperationState, time.Now()}, nil
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
				return status, nil
			}
			return nil, fmt.Errorf("Query failed execution: %s", status.state.String())
		}

		time.Sleep(5)
	}
}

func (r *Rows) Columns() []string {
	return []string{}
}

func (r *Rows) Close() error {
	r.operation = nil
	r.rowSet = nil
	r.offset = 0
	r.hasMore = false
	r.columns = nil

	return nil
}

func (r *Rows) Next(dest []driver.Value) error {
	return io.EOF
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

/*
func (r *hiveRows) Schema() []Column {
	if r.schema == nil {
		ret := make([]Column, len(r.columns))
		for i, col := range r.columns {
			ret[i] = Column{col}
		}
		return ret
		r.schema = ret
	}

	return r.schema
}

func convertColumnValue(column *tcliservice.TColumnValue) interface{} {
	var props = []string{"BoolVal", "StringVal", "ByteVal", "I16Val", "I32Val", "I64Val", "DoubleVal"}

	value := reflect.ValueOf(*column)
	for _, prop := range props {
		field := value.FieldByName(prop)
		fieldValue := field.FieldByName("Value")
		if fieldValue.IsValid() && fieldValue.Interface() != nil {
			log.Println(fieldValue.Interface())
		}
	}

	return nil
}

func (r *hiveRows) Next() ([]Value, error) {
	if r.rowSet == nil || r.offset >= len(r.rowSet.Rows) {
		if !r.hasMore {
			return nil, NoMoreRows
		}

		fetchReq := tcliservice.NewTFetchResultsReq()
		fetchReq.OperationHandle = *r.operation
		fetchReq.Orientation = tcliservice.TFetchOrientation_FETCH_NEXT
		fetchReq.MaxRows = fetch_max_rows

		resp, err := r.thriftClient.FetchResults(*fetchReq)
		if err != nil {
			return nil, err
		}

		if !isSuccessStatus(resp.Status) {
			return nil, fmt.Errorf("FetchResults failed: %s", resp.Status.String())
		}

		r.rowSet = resp.Results
		r.hasMore = *resp.HasMoreRows
	}

	row := r.rowSet.Rows[r.offset]
	ret := make([]Value, len(row.ColVals))
	for i, col := range row.ColVals {
		ret[i] = convertColumnValue(col)
	}

	r.offset++

	return ret, nil
}
*/
