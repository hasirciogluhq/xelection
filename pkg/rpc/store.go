package rpc

import (
	"context"
	"fmt"
	"sync"
	"time"

	packets "github.com/hasirciogluhq/xelection/pkg/packet"
)

type RpcHandler struct {
	Handler   func(packet *packets.Packet)
	Timeout   time.Duration `json:"timeout"`
	CreatedAt time.Time     `json:"created_at"`
}

type Store struct {
	mutex    *sync.RWMutex
	handlers map[int64]*RpcHandler
}

var (
	globalStore *Store
	storeOnce   sync.Once
)

func InitRpc() *Store {
	storeOnce.Do(func() {
		globalStore = &Store{
			handlers: make(map[int64]*RpcHandler),
			mutex:    &sync.RWMutex{},
		}
	})
	return globalStore
}

func GetRpcStore() *Store {
	if globalStore == nil {
		panic("rpc store not initialised")
	}
	return globalStore
}

func (s *Store) Start(ctx context.Context) {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	go func() {
		for {
			select {
			case <-ctx.Done():
				fmt.Println("RPC store stopped")
				return
			case <-ticker.C:
				if !s.mutex.TryLock() {
					continue
				}
				defer s.mutex.Unlock()
				for packetId, handler := range s.handlers {
					if time.Since(handler.CreatedAt) > handler.Timeout {
						delete(s.handlers, packetId)
					}
				}
			}
		}
	}()
}

func (s *Store) HandlePacket(packet *packets.Packet) error {
	handler, err := s.GetHandlerByRpcId(packet.Metadata["rpc_id"].(int64))
	if err != nil {
		return err
	}

	if !packet.IsRPC() {
		return fmt.Errorf("packet is not an rpc packet")
	}

	handler.Handler(packet)

	return nil
}

func (s *Store) RegisterHandler(packet *packets.Packet, handler *RpcHandler) int64 {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	s.handlers[packet.ID] = handler
	return packet.ID
}

func (s *Store) GetHandlerForPacketID(packetID int64) (*RpcHandler, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	handler, ok := s.handlers[packetID]
	if !ok {
		return nil, fmt.Errorf("handler not found")
	}
	return handler, nil
}

func (s *Store) GetHandlerByRpcId(rpcId int64) (*RpcHandler, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	handler, ok := s.handlers[rpcId]
	if !ok {
		return nil, fmt.Errorf("handler not found")
	}
	return handler, nil
}

func (s *Store) RemoveHandler(packet *packets.Packet) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	_, ok := s.handlers[packet.ID]
	if !ok {
		return fmt.Errorf("handler not found")
	}

	// delete now
	delete(s.handlers, packet.ID)
	return nil
}
