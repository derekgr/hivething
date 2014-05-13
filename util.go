package hivething

import (
	"github.com/derekgr/hivething/tcliservice"
)

func isSuccessStatus(p tcliservice.TStatus) bool {
	status := p.GetStatusCode()
	return status == tcliservice.TStatusCode_SUCCESS_STATUS || status == tcliservice.TStatusCode_SUCCESS_WITH_INFO_STATUS
}
