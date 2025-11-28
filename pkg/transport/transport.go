package transport

type TransportInterface interface {
	Send(bytes []byte) *error
}

type TransportManager struct {
	Listeners []TransportListener
}
