package impl

import (
    "bytes"
    "encoding/binary"
    "log"
    
    "persistence"
    . "command"
    . "client"
)

func NewMqttPubCompCommand() *MqttPubCompCommand {
    return &MqttPubCompCommand{
        fixedHeader : NewMqttFixedHeader(MQTT_CMD_PUBCOMP<<4, 0x00), 
        variableHeader : MqttPubCompVariableHeader{0},
    }
}

type MqttPubCompCommand struct {
    fixedHeader MqttFixedHeader
    variableHeader MqttPubCompVariableHeader
}

type MqttPubCompVariableHeader struct {
    packetId uint16
}

func (cmd *MqttPubCompCommand) Process(c Client) error {
    //receives MQTT PubComp command from client, remove msg list
    msgId, err := c.RemovePacketIdMapping(cmd.variableHeader.packetId)
    if err != nil {
        log.Print("MqttPubCompCommand failed to search packetId:", cmd.variableHeader.packetId)
        return err
    }
    return persistence.RemoveClientMessage(c.ClientId(), msgId)
}

func (cmd *MqttPubCompCommand) Parse(buf []byte, fixedHeader *MqttFixedHeader) (restBuf []byte, err error) {    
    cmd.fixedHeader = *fixedHeader
    
    //parse variable header    
    //packet id
    cmd.variableHeader.packetId = binary.BigEndian.Uint16(buf[0:2])
    
    restBuf = buf[2:]
    
    return restBuf, nil
}

func (cmd *MqttPubCompCommand) Buffer(buf *bytes.Buffer) error {
    cmd.fixedHeader.SetRemainLength(2)

    err := cmd.fixedHeader.Buffer(buf)
    if err != nil {
        return err
    }
    
    var packetId []byte = make([]byte, 2)
    binary.BigEndian.PutUint16(packetId, cmd.variableHeader.packetId)
    buf.Write(packetId)
    
    return nil
}

func (cmd *MqttPubCompCommand) SetPacketId(id int) {
    cmd.variableHeader.packetId = uint16(id)
}
