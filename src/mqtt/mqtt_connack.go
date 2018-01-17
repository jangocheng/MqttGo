package mqtt

import (
    "fmt"
    "bytes"
)

func NewMqttConnackCommand() *MqttConnackCommand {
    return &MqttConnackCommand{
        fixedHeader : MqttFixedHeader{0x20, 0x02}, 
        variableHeader : MqttConnackVariableHeader{true, 0x00},
    }
}

type MqttConnackCommand struct {
    fixedHeader MqttFixedHeader
    variableHeader MqttConnackVariableHeader
}

type MqttConnackVariableHeader struct {
    sessionPresent bool
    returnCode byte
}

func (cmd *MqttConnackCommand) Process(c *Client) error {
    fmt.Println("Process MQTT connack command: returnCode[", cmd.variableHeader.returnCode, "]")
    return nil
}

func (cmd *MqttConnackCommand) Parse(buf []byte, fixedHeader *MqttFixedHeader) (restBuf []byte, err error) {    
    return buf, nil
}

func (cmd *MqttConnackCommand) Buffer(buf *bytes.Buffer) error {
    err := cmd.fixedHeader.Buffer(buf)
    if err != nil {
        return nil
    }
    
    if cmd.variableHeader.sessionPresent {
        buf.WriteByte(byte(1))
    } else {
        buf.WriteByte(byte(0))
    }
    buf.WriteByte(cmd.variableHeader.returnCode)
    
    return nil
}

func (cmd *MqttConnackCommand) SetSessionPresent(sp bool) {
    cmd.variableHeader.sessionPresent = sp
}

func (cmd *MqttConnackCommand) SetReturnCode(code byte) {
    cmd.variableHeader.returnCode = code
}
