package main

import (
	"context"
	"time"

	packets "github.com/hasirciogluhq/xelection/pkg/packet"
	"github.com/hasirciogluhq/xelection/pkg/transport"
)

func CreateServer(ctx context.Context, manager *transport.TransportManager) transport.Transport {
	port := "4040"
	tcpTransport := transport.NewTcpTransport(ctx, manager, &port)

	go func() {
		err := tcpTransport.Start()
		if err != nil {
			panic(err)
		}
	}()

	return tcpTransport
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	transportManager := transport.NewTransportManager(ctx)
	transport := CreateServer(ctx, transportManager)

	time.Sleep(time.Millisecond * 200)

	client, err := transport.Dial("127.0.0.1", "4040")
	if err != nil {
		panic(err)
	}

	somePacket := packets.BuildPacket(packets.PacketTypeEvent, "test", nil, nil)

	err = transportManager.SendData(client, somePacket)
	if err != nil {
		panic(err)
	}

	err = transportManager.SendData(client, somePacket)
	if err != nil {
		panic(err)
	}

	err = transportManager.SendData(client, somePacket)
	if err != nil {
		panic(err)
	}

	err = transportManager.SendData(client, somePacket)
	if err != nil {
		panic(err)
	}

	err = transportManager.SendData(client, somePacket)
	if err != nil {
		panic(err)
	}

	time.Sleep(time.Millisecond * 100)

	for _, client := range transportManager.Clients {
		client.Disconnect()
	}

	time.Sleep(time.Second * 1)

	cancel()
}
