package impl

import (
    "bytes"
    
    . "command"
    . "client"
)

func NewMqttPingRespCommand() *MqttPingRespCommand {
    return &MqttPingRespCommand{
        fixedHeader : NewMqttFixedHeader(MQTT_CMD_PINGRESP<<4, 0),
    }
}

type MqttPingRespCommand struct {
    fixedHeader MqttFixedHeader
}

func (cmd *MqttPingRespCommand) Process(c Client) error {
    return nil
}

func (cmd *MqttPingRespCommand) Parse(buf []byte, fixedHeader *MqttFixedHeader) (restBuf []byte, err error) {    
    return buf, nil
}

func (cmd *MqttPingRespCommand) Buffer(buf *bytes.Buffer) error {
    err := cmd.fixedHeader.Buffer(buf)
    return err
}
