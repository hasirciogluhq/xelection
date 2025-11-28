package transport

type Client struct {
	Transport *TransportInterface // Burası transport connection işaret edecek direkt olarak client.(Transport).Send ile gönderilebilecek.
}

func NewTransportClient(transport *TransportInterface) *Client {
	return &Client{
		Transport: transport,
	}
}
