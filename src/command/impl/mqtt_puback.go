package impl

import (
    "bytes"
    "encoding/binary"
    "log"
    
    "persistence"
    . "command"
    . "client"
)

func NewMqttPubackCommand() *MqttPubackCommand {
    return &MqttPubackCommand{
        fixedHeader : NewMqttFixedHeader(MQTT_CMD_PUBACK<<4, 0), 
        variableHeader : MqttPubackVariableHeader{0},
    }
}

type MqttPubackCommand struct {
    fixedHeader MqttFixedHeader
    variableHeader MqttPubackVariableHeader
}

type MqttPubackVariableHeader struct {
    packetId uint16
}

func (cmd *MqttPubackCommand) Process(c Client) error {
    log.Print("MqttPubackCommand ClientId[", c.ClientId(), "] PacketId[", cmd.variableHeader.packetId, "]")
    if !c.CleanSession() {
        msgId, err := c.RemovePacketIdMapping(cmd.variableHeader.packetId)
        if err == nil {
            err = persistence.RemoveClientMessage(c.ClientId(), msgId)
            if err != nil {
                log.Print("RemoveClientMessage failed, clientId[", c.ClientId(), "] msgId[", msgId, "] error:", err)
            }
        }
    }
    return nil
}

func (cmd *MqttPubackCommand) Parse(buf []byte, fixedHeader *MqttFixedHeader) (restBuf []byte, err error) {    
    cmd.fixedHeader = *fixedHeader
    
    //parse variable header    
    //packet id
    cmd.variableHeader.packetId = binary.BigEndian.Uint16(buf[0:2])
    
    restBuf = buf[2:]
    
    return
}

func (cmd *MqttPubackCommand) Buffer(buf *bytes.Buffer) error {
    cmd.fixedHeader.SetRemainLength(2)

    err := cmd.fixedHeader.Buffer(buf)
    if err != nil {
        return nil
    }
    
    var packetId []byte = make([]byte, 2)
    binary.BigEndian.PutUint16(packetId, cmd.variableHeader.packetId)
    buf.Write(packetId)
    
    return nil
}

func (cmd *MqttPubackCommand) SetPacketId(id int) {
    cmd.variableHeader.packetId = uint16(id)
}
