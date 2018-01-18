package mqtt

import (
    "bytes"
)

type MqttPingReqCommand struct {
    fixedHeader MqttFixedHeader
}

func (cmd *MqttPingReqCommand) Process(c *Client) error {
    ackCmd := NewMqttPingRespCommand()
    c.SendCommand(ackCmd)
    return nil
}

func (cmd *MqttPingReqCommand) Parse(buf []byte, fixedHeader *MqttFixedHeader) (restBuf []byte, err error) {
    cmd.fixedHeader = *fixedHeader
    return buf, nil
}

func (cmd *MqttPingReqCommand) Buffer(buf *bytes.Buffer) error {    
    return nil
}
