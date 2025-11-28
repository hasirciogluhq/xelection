package packets

import (
	"encoding/json"
	"time"

	"github.com/hasirciogluhq/xelection/pkg/utils"
)

type PacketType string

const (
	PacketTypeEvent PacketType = "EVENT"
	PacketTypeRPC   PacketType = "RPC"
)

// Packet represents a unified packet from or to transport endpoints.
type Packet struct {
	ID         int64                  `json:"id"`
	PacketType PacketType             `json:"packet_type"`
	Metadata   map[string]interface{} `json:"metadata"`
	Payload    map[string]interface{} `json:"payload"`
	Timestamp  int64                  `json:"timestamp"`
}

// ParsePacket parses a JSON packet payload into Packet.
func ParsePacketFromBytes(data []byte) (*Packet, error) {
	var packet Packet
	if err := json.Unmarshal(data, &packet); err != nil {
		return nil, err
	}
	return &packet, nil
}

// IsEvent returns true if packet is EVENT type
func (p *Packet) IsEvent() bool {
	return p.PacketType == PacketTypeEvent
}

// IsRPC returns true if packet is RPC type
func (p *Packet) IsRPC() bool {
	return p.PacketType == PacketTypeRPC
}

func (p *Packet) IsRPCRequest() bool {
	v, ok := p.Metadata["rpc_id"].(string)
	return p.IsRPC() && (!ok || v == "")
}

func (p *Packet) IsRPCResponse() bool {
	v, ok := p.Metadata["rpc_id"].(string)
	return p.IsRPC() && ok && v != ""
}

// GetAction returns the action from metadata
func (p *Packet) GetAction() string {
	if action, ok := p.Metadata["action"].(string); ok {
		return action
	}
	return ""
}

// BuildPacket creates a packet with the provided type/action/payload combination.
// If packetType is invalid it defaults to EVENT packets.
func BuildPacket(packetType PacketType, action string, payload map[string]interface{}, metadata map[string]interface{}) *Packet {
	if packetType != PacketTypeEvent && packetType != PacketTypeRPC {
		packetType = PacketTypeEvent
	}

	if payload == nil {
		payload = map[string]interface{}{}
	}

	if metadata == nil {
		metadata = map[string]interface{}{}
	}

	if action != "" {
		metadata["action"] = action
	}

	return &Packet{
		ID:         utils.GenerateSnowflakeID(),
		PacketType: packetType,
		Metadata:   metadata,
		Payload:    payload,
		Timestamp:  time.Now().Unix(),
	}
}

// ToJSON converts the packet to a JSON string
func (p *Packet) ToBytes() []byte {
	bytes, err := json.Marshal(p)
	if err != nil {
		return nil
	}
	return bytes
}
