package impl

import (
    "bytes"
    "encoding/binary"
    "log"
    
    "persistence"
    . "command"
    . "client"
)

func NewMqttPubRecCommand() *MqttPubRecCommand {
    return &MqttPubRecCommand{
        fixedHeader : NewMqttFixedHeader(MQTT_CMD_PUBREC<<4, 0x00), 
        variableHeader : MqttPubRecVariableHeader{0},
    }
}

type MqttPubRecCommand struct {
    fixedHeader MqttFixedHeader
    variableHeader MqttPubRecVariableHeader
}

type MqttPubRecVariableHeader struct {
    packetId uint16
}

func (cmd *MqttPubRecCommand) Process(c Client) error {
    log.Print("MqttPubRecCommand ClientId[", c.ClientId(), "] PacketId[", cmd.variableHeader.packetId, "]")
    //client receives a Qos=2 publish message
    _, err := persistence.UpdateQos2MessageStatus(c.ClientId(), int(cmd.variableHeader.packetId))
    if err != nil {
        return err
    }
    
    //send PubRel command
    pubRelCmd := NewMqttPubRelCommand()
    pubRelCmd.SetPacketId(int(cmd.variableHeader.packetId))
    c.SendCommand(pubRelCmd)
    return nil
}

func (cmd *MqttPubRecCommand) Parse(buf []byte, fixedHeader *MqttFixedHeader) (restBuf []byte, err error) {    
    cmd.fixedHeader = *fixedHeader
    
    //parse variable header    
    //packet id
    cmd.variableHeader.packetId = binary.BigEndian.Uint16(buf[0:2])
    
    restBuf = buf[2:]
    
    return restBuf, nil
}

func (cmd *MqttPubRecCommand) Buffer(buf *bytes.Buffer) error {
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

func (cmd *MqttPubRecCommand) SetPacketId(id int) {
    cmd.variableHeader.packetId = uint16(id)
}
