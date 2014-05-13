package hivething

import (
	"database/sql/driver"
	"errors"
	"git.apache.org/thrift.git/lib/go/thrift"
	"github.com/derekgr/hivething/tcliservice"
)

// Options for opened Hive sessions.
type Options struct {
	PollIntervalSeconds int
	BatchSize           int
}

var (
	DriverDefaults = Options{PollIntervalSeconds: 5, BatchSize: 10000}
)

type Driver struct {
	options Options
}

func NewDriver(options Options) driver.Driver {
	return &Driver{options}
}

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
		return nil, err
	}

	return &Connection{client, session.SessionHandle, d.options}, nil
}
