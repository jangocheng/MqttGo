package impl

import (
    "bytes"
    "encoding/binary"
    
    . "command"
    . "client"
)

func NewMqttUnSubackCommand() *MqttUnSubackCommand {
    return &MqttUnSubackCommand{
        fixedHeader : NewMqttFixedHeader(MQTT_CMD_UNSUBACK<<4, 0x00), 
        variableHeader : MqttUnSubackVariableHeader{0},
    }
}

type MqttUnSubackCommand struct {
    fixedHeader MqttFixedHeader
    variableHeader MqttUnSubackVariableHeader
}

type MqttUnSubackVariableHeader struct {
    packetId uint16
}

func (cmd *MqttUnSubackCommand) Process(c Client) error {
    return nil
}

func (cmd *MqttUnSubackCommand) Parse(buf []byte, fixedHeader *MqttFixedHeader) (restBuf []byte, err error) {    
    return buf, nil
}

func (cmd *MqttUnSubackCommand) Buffer(buf *bytes.Buffer) error {
    remainLength := 2 //packet id length
    cmd.fixedHeader.SetRemainLength(remainLength)

    err := cmd.fixedHeader.Buffer(buf)
    if err != nil {
        return nil
    }
    
    var packetId []byte = make([]byte, 2)
    binary.BigEndian.PutUint16(packetId, cmd.variableHeader.packetId)
    buf.Write(packetId)
    
    return nil
}

func (cmd *MqttUnSubackCommand) SetPacketId(id int) {
    cmd.variableHeader.packetId = uint16(id)
}
