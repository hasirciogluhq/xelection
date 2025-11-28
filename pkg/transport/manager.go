package transport

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/hasirciogluhq/xelection/pkg/logger"
	packets "github.com/hasirciogluhq/xelection/pkg/packet"
	"github.com/hasirciogluhq/xelection/pkg/rpc"
)

type TransportManager struct {
	ctx        context.Context
	transports []Transport
	mu         sync.RWMutex
	Clients    map[int64]*Client // client ID -> client
	handlers   []PacketHandler
}

func NewTransportManager(ctx context.Context) *TransportManager {
	return &TransportManager{
		ctx:        ctx,
		transports: make([]Transport, 0),
		Clients:    map[int64]*Client{},
		mu:         sync.RWMutex{},
		handlers:   make([]PacketHandler, 0),
	}
}

func (tm *TransportManager) onConnected(client *Client, conn interface{}) {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	tm.Clients[client.ID] = client
	logger.Info("transport: client registered [id=%s]", client.ID)

	// Notify handler about connection
	for _, handler := range tm.handlers {
		if handler != nil {
			if handler.PacketHandlerFilter(client) {
				go handler.HandleConnect(client)
			}
		}
	}
}

func (tm *TransportManager) onError(client *Client, err error) {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	client.Disconnect()
}

func (tm *TransportManager) onDataReceived(client *Client, data []byte) {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	var packet *packets.Packet
	parseErr := json.Unmarshal(data, packet)
	if parseErr == nil {
		client.Disconnect()
		return
	}

	// Notify handler about connection
	for _, handler := range tm.handlers {
		if handler != nil {
			if handler.PacketHandlerFilter(client) {
				go handler.HandlePacket(client, packet)
			}
		}
	}
}

func (tm *TransportManager) onDisconnected(client *Client, conn interface{}) {
	tm.mu.Lock()
	_, exists := tm.Clients[client.ID]
	if exists {
		delete(tm.Clients, client.ID)
	}
	tm.mu.Unlock()

	if exists {
		logger.Info("transport: client unregistered [id=%s]", client.ID)
		// Notify handler about disconnection
		for _, handler := range tm.handlers {
			if handler != nil {
				if handler.PacketHandlerFilter(client) {
					go handler.HandleDisconnect(client)
				}
			}
		}
	}
}

// SendData sends raw data to a client
func (tm *TransportManager) SendData(client *Client, packet *packets.Packet) error {
	tm.mu.RLock()
	_, exists := tm.Clients[client.ID]
	tm.mu.RUnlock()

	if !exists {
		logger.Warn("transport: client not found [clientID=%s]", client.ID)
		return ErrClientNotFound
	}

	// Get transport sender from client's transport data
	if sender, ok := client.TransportData.(Sender); ok {
		data := packet.ToBytes()
		if data == nil {
			logger.Error("transport: failed to serialize packet [clientID=%s, packetID=%s, packetAction=%s]", client.ID, packet.ID, packet.GetAction())
			return fmt.Errorf("failed to serialize packet")
		}
		logger.Debug("transport: sending packet [clientID=%s, packetID=%s, action=%s, size=%d]",
			client.ID, packet.ID, packet.GetAction(), len(data))
		if err := sender.Send(data); err != nil {
			logger.Warn("transport: failed to send packet [clientID=%s, error=%v]", client.ID, err)
			return err
		}
		logger.Debug("transport: packet sent successfully [clientID=%s, packetID=%s]", client.ID, packet.ID)
		return nil
	}

	logger.Error("transport: client transport does not support sending [clientID=%s, transportType=%T]", client.ID, client.TransportData)
	return fmt.Errorf("client transport does not support sending")
}

func (tm *TransportManager) SendRpcRequestPacket(client *Client, packet *packets.Packet) (*packets.Packet, error) {
	timeout := time.NewTimer(10 * time.Second)
	defer timeout.Stop()
	packetChan := make(chan *packets.Packet, 1)
	defer close(packetChan)

	_ = rpc.GetRpcStore().RegisterHandler(packet, &rpc.RpcHandler{
		Handler: func(packet *packets.Packet) {
			packetChan <- packet
		},
		Timeout:   30 * time.Second,
		CreatedAt: time.Now(),
	})

	err := tm.SendData(client, packet)
	if err != nil {
		rpc.GetRpcStore().RemoveHandler(packet)
		return nil, err
	}

	select {
	case <-tm.ctx.Done():
		return nil, errors.New("ctx done")
	case rpcCBPacket := <-packetChan:
		rpc.GetRpcStore().RemoveHandler(packet)
		return rpcCBPacket, nil
	case <-timeout.C:
		rpc.GetRpcStore().RemoveHandler(packet)
		return nil, errors.New("timeout")
	}
}

func (tm *TransportManager) SendRpcResponsePacket(client *Client, requestPacket *packets.Packet, responsePacket *packets.Packet) error {
	// bunu direk gönder gitsin moruk ama minik rpc id gibi şeyler eklemen gerek.
	responsePacket.Metadata["rpc_id"] = requestPacket.ID
	return tm.SendData(client, responsePacket)
}
