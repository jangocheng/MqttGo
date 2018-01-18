package mqtt

import (
    "bytes"
)

func NewMqttPingRespCommand() *MqttPingRespCommand {
    return &MqttPingRespCommand{
        fixedHeader : MqttFixedHeader{0xd0, 0x00},
    }
}

type MqttPingRespCommand struct {
    fixedHeader MqttFixedHeader
}

func (cmd *MqttPingRespCommand) Process(c *Client) error {
    return nil
}

func (cmd *MqttPingRespCommand) Parse(buf []byte, fixedHeader *MqttFixedHeader) (restBuf []byte, err error) {    
    return buf, nil
}

func (cmd *MqttPingRespCommand) Buffer(buf *bytes.Buffer) error {
    err := cmd.fixedHeader.Buffer(buf)
    return err
}
