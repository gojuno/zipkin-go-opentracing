package zipkintracer

import (
	"bytes"
	"net/http"
	"sync"
	"time"

	"github.com/apache/thrift/lib/go/thrift"

	"github.com/openzipkin/zipkin-go-opentracing/_thrift/gen-go/zipkincore"
)

// Default timeout for http request in seconds
const defaultHTTPTimeout = time.Second * 5

// defaultHTTPBatchInterval in seconds
const defaultHTTPBatchInterval = 1

const defaultHTTPBatchSize = 100

const defaultHTTPMaxBacklog = 1000

// HTTPCollector implements Collector by forwarding spans to a http server.
type HTTPCollector struct {
	logger        Logger
	url           string
	client        *http.Client
	batchInterval time.Duration
	batchSize     int
	batch         []*zipkincore.Span
	spanc         chan *zipkincore.Span
	quit          chan struct{}
	shutdown      chan error
	batchPool     sync.Pool
}

// HTTPOption sets a parameter for the HttpCollector
type HTTPOption func(c *HTTPCollector)

// HTTPLogger sets the logger used to report errors in the collection
// process. By default, a no-op logger is used, i.e. no errors are logged
// anywhere. It's important to set this option in a production service.
func HTTPLogger(logger Logger) HTTPOption {
	return func(c *HTTPCollector) { c.logger = logger }
}

// HTTPTimeout sets maximum timeout for http request.
func HTTPTimeout(duration time.Duration) HTTPOption {
	return func(c *HTTPCollector) { c.client.Timeout = duration }
}

// HTTPBatchSize sets the maximum batch size, after which a collect will be
// triggered. The default batch size is 100 traces.
func HTTPBatchSize(n int) HTTPOption {
	return func(c *HTTPCollector) {
		c.batchSize = n
		c.spanc = make(chan *zipkincore.Span, n)
		c.batchPool = sync.Pool{
			New: func() interface{} {
				return make([]*zipkincore.Span, 0, n)
			},
		}
	}
}

// HTTPBatchInterval sets the maximum duration we will buffer traces before
// emitting them to the collector. The default batch interval is 1 second.
func HTTPBatchInterval(d time.Duration) HTTPOption {
	return func(c *HTTPCollector) { c.batchInterval = d }
}

// HTTPClient sets a custom http client to use.
func HTTPClient(client *http.Client) HTTPOption {
	return func(c *HTTPCollector) { c.client = client }
}

// NewHTTPCollector returns a new HTTP-backend Collector. url should be a http
// url for handle post request. timeout is passed to http client. queueSize control
// the maximum size of buffer of async queue. The logger is used to log errors,
// such as send failures;
func NewHTTPCollector(url string, options ...HTTPOption) (Collector, error) {
	c := &HTTPCollector{
		logger:        NewNopLogger(),
		url:           url,
		client:        &http.Client{Timeout: defaultHTTPTimeout},
		batchInterval: defaultHTTPBatchInterval * time.Second,
		batchSize:     defaultHTTPBatchSize,
		batch:         []*zipkincore.Span{},
		spanc:         make(chan *zipkincore.Span, defaultHTTPBatchSize),
		quit:          make(chan struct{}, 1),
		shutdown:      make(chan error, 1),
		batchPool: sync.Pool{
			New: func() interface{} {
				return make([]*zipkincore.Span, 0, defaultHTTPBatchSize)
			},
		},
	}

	for _, option := range options {
		option(c)
	}

	go c.loop()
	return c, nil
}

// Collect implements Collector.
func (c *HTTPCollector) Collect(s *zipkincore.Span) error {
	c.spanc <- s
	return nil
}

// Close implements Collector.
func (c *HTTPCollector) Close() error {
	close(c.quit)
	return <-c.shutdown
}

func httpSerialize(spans []*zipkincore.Span) (*bytes.Buffer, error) {
	t := thrift.NewTMemoryBuffer()
	p := thrift.NewTBinaryProtocolTransport(t)
	if err := p.WriteListBegin(thrift.STRUCT, len(spans)); err != nil {
		return nil, err
	}
	for _, s := range spans {
		if err := s.Write(p); err != nil {
			return nil, err
		}
	}
	if err := p.WriteListEnd(); err != nil {
		return nil, err
	}
	return t.Buffer, nil
}

func (c *HTTPCollector) loop() {
	var (
		nextSend = time.Now().Add(c.batchInterval)
		ticker   = time.NewTicker(c.batchInterval / 10)
	)
	defer ticker.Stop()

	b := c.createBuffer()

	for {
		select {
		case span := <-c.spanc:
			b = append(b, span)
			if len(b) >= c.batchSize {
				nextSend = time.Now().Add(c.batchInterval)
				go c.flush(b)
				b = c.createBuffer()
			}
		case <-ticker.C:
			if time.Now().After(nextSend) {
				nextSend = time.Now().Add(c.batchInterval)
				go c.flush(b)
				b = c.createBuffer()
			}
		case <-c.quit:
			c.shutdown <- c.flush(b)
			return
		}
	}
}

// createBuffer returns new batch
func (c *HTTPCollector) createBuffer() []*zipkincore.Span {
	return c.batchPool.Get().([]*zipkincore.Span)
}

// flush sends spans to zipkin. If some error happens while sending then spans are discarded.
func (c *HTTPCollector) flush(b []*zipkincore.Span) (err error) {
	defer func() {
		c.batchPool.Put(b[:0])
		if err != nil {
			c.logger.Log("err", err)
		}
	}()

	// Do not send an empty batch
	if len(b) == 0 {
		return nil
	}

	data, err := httpSerialize(b)
	if err != nil {
		return
	}

	var req *http.Request

	req, err = http.NewRequest("POST", c.url, data)
	if err != nil {
		return
	}

	req.Header.Set("Content-Type", "application/x-thrift")
	if _, err = c.client.Do(req); err != nil {
		return
	}

	return nil
}
