package transport

import (
	"sync"

	"github.com/hasirciogluhq/xelection/pkg/utils"
)

type DisConnector interface {
	Disconnect() error
}

type Sender interface {
	Send(data []byte) error
}

type ClientConnectionDirection string

var (
	ClientConnectionDirectionIncoming ClientConnectionDirection = "incoming"
	ClientConnectionDirectionOutgoing ClientConnectionDirection = "outgoing"
)

type Client struct {
	ID                  int64
	mu                  sync.RWMutex
	ConnectionDirection ClientConnectionDirection
	Context             map[string]interface{}
	TransportData       interface{} // Burası transport connection işaret edecek direkt olarak client.(Transport).Send ile gönderilebilecek.
}

func NewTransportClient(transData interface{}, direction ClientConnectionDirection) *Client {
	return &Client{
		ID:                  utils.GenerateSnowflakeID(),
		TransportData:       transData,
		ConnectionDirection: direction,
		Context:             make(map[string]interface{}),
	}
}

// SetContext sets a context value
func (c *Client) SetContext(key string, value interface{}) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.Context[key] = value
}

// GetContext gets a context value
func (c *Client) GetContext(key string) (interface{}, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	value, ok := c.Context[key]
	return value, ok
}

// GetContextString gets a context value as string
func (c *Client) GetContextString(key string) (string, bool) {
	value, ok := c.GetContext(key)
	if !ok {
		return "", false
	}
	str, ok := value.(string)
	return str, ok
}

func (c *Client) GetContextBool(key string) (bool, bool) {
	value, ok := c.GetContext(key)
	if !ok {
		return false, false
	}
	b, ok := value.(bool)
	return b, ok
}

func (c *Client) GetContextInt(key string) (int, bool) {
	value, ok := c.GetContext(key)
	if !ok {
		return 0, false
	}
	i, ok := value.(int)
	return i, ok
}

func (c *Client) GetContextInt64(key string) (int64, bool) {
	value, ok := c.GetContext(key)
	if !ok {
		return 0, false
	}
	i, ok := value.(int64)
	return i, ok
}

func (c *Client) GetContextFloat64(key string) (float64, bool) {
	value, ok := c.GetContext(key)
	if !ok {
		return 0, false
	}
	f, ok := value.(float64)
	return f, ok
}

func (c *Client) GetContextMap(key string) (map[string]interface{}, bool) {
	value, ok := c.GetContext(key)
	if !ok {
		return nil, false
	}
	m, ok := value.(map[string]interface{})
	return m, ok
}

func (c *Client) GetContextSlice(key string) ([]interface{}, bool) {
	value, ok := c.GetContext(key)
	if !ok {
		return nil, false
	}
	slice, ok := value.([]interface{})
	return slice, ok
}

// Setters - eksik olanlar ekleniyor

func (c *Client) SetContextString(key, value string) {
	c.SetContext(key, value)
}

func (c *Client) SetContextBool(key string, value bool) {
	c.SetContext(key, value)
}

func (c *Client) SetContextInt(key string, value int) {
	c.SetContext(key, value)
}

func (c *Client) SetContextInt64(key string, value int64) {
	c.SetContext(key, value)
}

func (c *Client) SetContextFloat64(key string, value float64) {
	c.SetContext(key, value)
}

func (c *Client) SetContextMap(key string, value map[string]interface{}) {
	c.SetContext(key, value)
}

func (c *Client) SetContextSlice(key string, value []interface{}) {
	c.SetContext(key, value)
}

func (c *Client) IsAuthenticated() bool {
	authenticated, ok := c.GetContextBool("authenticated")
	return ok && authenticated
}

func (c *Client) SetAuthenticated(authenticated bool) {
	c.SetContext("authenticated", authenticated)
}

// Disconnect disconnects the client through its transport layer
func (c *Client) Disconnect() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Disconnect through transport layer
	if disConnector, ok := c.TransportData.(DisConnector); ok {
		if err := disConnector.Disconnect(); err != nil {
			return err
		}
	}

	return nil
}
