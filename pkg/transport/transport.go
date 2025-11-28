package transport

import (
	"errors"

	packets "github.com/hasirciogluhq/xelection/pkg/packet"
)

type TransportClientDispatcher interface {
	onConnected(client *Client, conn interface{})
	onDisconnected(Client *Client, conn interface{})
	onDataReceived(Client *Client, data []byte)
	onError(Client *Client, err error)
}

var (
	// ErrClientChannelFull = errors.New("client channel full") // this is for websocket
	ErrClientNotFound = errors.New("client not found")
)

// PacketHandler handles incoming packets - completely abstract
type PacketHandler interface {
	PacketHandlerFilter(client *Client) bool
	HandlePacket(client *Client, packet *packets.Packet) error
	HandleDisconnect(client *Client)
	HandleConnect(client *Client)
}

type TransportDialer interface {
	Dial(...any) (*Client, error)
}

type Transport interface {
	Start() error
	Shutdown() error
	TransportDialer
}
