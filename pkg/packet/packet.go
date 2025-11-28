package packet

import (
	"errors"

	"github.com/hasirciogluhq/xelection/pkg/utils"
)

type Packet struct {
	ID       int64          `json:"id"`
	Payload  map[string]any `json:"payload,omitempty"`
	Metadata map[string]any `json:"metadata,omitempty"`
}

func NewPacket(payload map[string]any, metadata map[string]any) (*Packet, error) {
	generatedPacket := &Packet{
		ID:       utils.GenerateSnowflakeID(),
		Payload:  payload,
		Metadata: metadata,
	}

	data, isOk := metadata["action"].(string)

	if !isOk || data == "" {
		return nil, errors.New("Metadata action must be defined")
	}

	return generatedPacket, nil
}

func NewPacketWithAction(action string, payload map[string]any, metadata map[string]any) (*Packet, error) {
	generatedPacket := &Packet{
		ID:       utils.GenerateSnowflakeID(),
		Payload:  payload,
		Metadata: metadata,
	}

	metadata["action"] = action

	return generatedPacket, nil
}
