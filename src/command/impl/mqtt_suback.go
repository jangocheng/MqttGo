package impl

import (
    "bytes"
    "encoding/binary"
    
    . "command"
    . "client"
)

func NewMqttSubackCommand() *MqttSubackCommand {
    return &MqttSubackCommand{
        fixedHeader : NewMqttFixedHeader(MQTT_CMD_SUBACK<<4, 0x00), 
        variableHeader : MqttSubackVariableHeader{0},
        payload : MqttSubackPayload{nil},
    }
}

type MqttSubackCommand struct {
    fixedHeader MqttFixedHeader
    variableHeader MqttSubackVariableHeader
    payload MqttSubackPayload
}

type MqttSubackVariableHeader struct {
    packetId uint16
}

type MqttSubackPayload struct {
    returnCodes []byte
}

func (cmd *MqttSubackCommand) Process(c Client) error {
    return nil
}

func (cmd *MqttSubackCommand) Parse(buf []byte, fixedHeader *MqttFixedHeader) (restBuf []byte, err error) {    
    return buf, nil
}

func (cmd *MqttSubackCommand) Buffer(buf *bytes.Buffer) error {
    remainLength := 2 //packet id length
    remainLength += len(cmd.payload.returnCodes)
    cmd.fixedHeader.SetRemainLength(remainLength)

    err := cmd.fixedHeader.Buffer(buf)
    if err != nil {
        return nil
    }
    
    var packetId []byte = make([]byte, 2)
    binary.BigEndian.PutUint16(packetId, cmd.variableHeader.packetId)
    buf.Write(packetId)
    
    buf.Write(cmd.payload.returnCodes)
    
    return nil
}

func (cmd *MqttSubackCommand) SetPacketId(id int) {
    cmd.variableHeader.packetId = uint16(id)
}

func (cmd *MqttSubackCommand) SetReturnCodesLength(length int) {
    cmd.payload.returnCodes = make([]byte, length)
}

func (cmd *MqttSubackCommand) SetReturnCodes(index int, qos byte) {
    cmd.payload.returnCodes[index] = qos
}
