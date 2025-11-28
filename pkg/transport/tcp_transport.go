package transport

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"time"

	"github.com/hasirciogluhq/xelection/pkg/logger"
	"github.com/hasirciogluhq/xelection/pkg/utils"
)

const (
	defaultPort     = "8081"
	readTimeout     = 5 * time.Minute  // 5 minutes - long timeout, TCP keep-alive handles connection health
	keepAlivePeriod = 30 * time.Second // TCP keep-alive every 30 seconds (OS level)
)

// TCPClient wraps a TCP connection
type TCPClient struct {
	conn     net.Conn
	clientID int64
}

// NewTCPClient creates a new TCP client
func NewTCPClient(conn net.Conn) *TCPClient {
	return &TCPClient{
		conn:     conn,
		clientID: utils.GenerateSnowflakeID(),
	}
}

// Send implements PacketSender interface
func (tc *TCPClient) Send(data []byte) error {
	// Write length prefix (4 bytes)
	length := uint32(len(data))
	if err := binary.Write(tc.conn, binary.BigEndian, length); err != nil {
		return err
	}

	// Write data
	_, err := tc.conn.Write(data)
	return err
}

// Disconnect implements transport.Disconnecter
func (tc *TCPClient) Disconnect() error {
	return tc.conn.Close()
}

type TCPTransport struct {
	ctx        context.Context
	dispatcher TransportClientDispatcher
	port       *string
	listener   net.Listener
}

func NewTcpTransport(ctx context.Context, dispatcher TransportClientDispatcher, port *string) Transport {
	return &TCPTransport{
		ctx:        ctx,
		dispatcher: dispatcher,
		port:       port,
		listener:   &net.TCPListener{},
	}
}

func (ttl *TCPTransport) Start() error {
	var port string
	if ttl.port == nil {
		port = defaultPort
	} else {
		port = *ttl.port
	}
	var err error = nil
	ttl.listener, err = net.Listen("tcp", ":"+port)
	if err != nil {
		return errors.New("failed to listen on port " + port + ": " + err.Error())
	}

	defer ttl.listener.Close()

	for {
		select {
		case <-ttl.ctx.Done():
			fmt.Println("Ctx done")
			return nil

		default:
			conn, err := ttl.listener.Accept()
			if err != nil {
				fmt.Println("failed to accept connection: " + err.Error())
				continue
			}

			_, err = ttl.HandleTCPConnection(conn, ClientConnectionDirectionIncoming)
			if err != nil {
				fmt.Println("failed to handle connection: " + err.Error())
				continue
			}
		}
	}
}

func (ttl *TCPTransport) Shutdown() error {
	return ttl.listener.Close()
}

// HandleTCPConnection handles a TCP connection
func (ttl *TCPTransport) HandleTCPConnection(conn net.Conn, direction ClientConnectionDirection) (*Client, error) {
	tcpClient := NewTCPClient(conn)

	var context string
	if direction == ClientConnectionDirectionIncoming {
		context = "tcp-server"
	} else {
		context = "tcp-client"
	}

	// Create transport client
	transportClient := NewTransportClient(tcpClient, ClientConnectionDirectionIncoming)
	transportClient.SetAuthenticated(false)
	ttl.dispatcher.onConnected(transportClient, conn)
	logger.Info("%s: client connected to server [clientID=%d, remoteAddr=%s]", context, transportClient.ID, conn.RemoteAddr())

	// Read loop
	go func() {
		ttl.readLoop(conn, transportClient, context)
	}()

	return transportClient, nil
}

func (ttl *TCPTransport) readLoop(conn net.Conn, client *Client, loopContext string) {
	defer func() {
		ttl.dispatcher.onDisconnected(client, conn)
		conn.Close()
		logger.Info("tcp-server: client disconnected [clientID=%d]", client.ID)
	}()

	// Set TCP keep-alive - OS level mechanism that sends keep-alive packets
	// every 30 seconds to detect dead connections. This is independent of read timeout.
	if tcpConn, ok := conn.(*net.TCPConn); ok {
		tcpConn.SetKeepAlive(true)
		tcpConn.SetKeepAlivePeriod(keepAlivePeriod)
	}
	// READ_LOOP:s
	for {
		select {
		case <-ttl.ctx.Done():
			conn.Close()
			fmt.Println("Ctx done")
			return
		default:
			// Read timeout: Application level - if no data received in 5 minutes, timeout
			// TCP keep-alive: OS level - checks if connection is physically alive every 30s
			// They work independently - keep-alive ensures connection health, read timeout
			// ensures we don't block forever if no data comes
			conn.SetReadDeadline(time.Now().Add(readTimeout))

			// Read length prefix (4 bytes)
			var length uint32
			if err := binary.Read(conn, binary.BigEndian, &length); err != nil {
				// Check if it's a timeout error
				if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
					// Timeout - connection might still be alive, just no data
					// Reset deadline and continue
					logger.Debug("%s: read timeout (connection still alive) [clientID=%d]", loopContext, client.ID)
					continue
				}

				if err == io.EOF {
					logger.Debug("%s: client disconnected [clientID=%d]", loopContext, client.ID)
				} else if strings.Contains(err.Error(), "use of closed network connection") {
					return
				} else if err == net.ErrClosed {
					logger.Debug("Read from closed conn")
				} else {
					logger.Warn("%s: failed to read length [clientID=%d, error=%v]", loopContext, client.ID, err)
				}
				return
			}

			// Validate length (max 10MB)
			if length > 10*1024*1024 {
				logger.Warn("%s: packet too large [clientID=%d, length=%d]", loopContext, client.ID, length)
				break
			}

			// Read packet data with deadline reset
			conn.SetReadDeadline(time.Now().Add(readTimeout))
			data := make([]byte, length)
			if _, err := io.ReadFull(conn, data); err != nil {
				if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
					logger.Debug("%s: read timeout while reading packet data [clientID=%d]", loopContext, client.ID)
					continue
				}
				logger.Warn("%s: failed to read packet data [clientID=%d, error=%v]", loopContext, client.ID, err)
				break
			}

			fmt.Println("REad OK")

			ttl.dispatcher.onDataReceived(client, data)
		}
	}
}

func (ttl *TCPTransport) Dial(dialParams ...any) (*Client, error) {
	addr, addrOk := dialParams[0].(string)
	port, portOk := dialParams[1].(string)

	if !addrOk || !portOk {
		return nil, errors.New("wrong tcp dial parameters")
	}

	tcpAddr, err := net.ResolveTCPAddr("tcp", addr+":"+port)
	if err != nil {
		return nil, err
	}

	conn, err := net.DialTCP("tcp", nil, tcpAddr)
	if err != nil {
		return nil, err
	}

	return ttl.HandleTCPConnection(conn, ClientConnectionDirectionOutgoing)
}
