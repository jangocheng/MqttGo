package mqtt

import (
    "fmt"
    "bytes"
    "encoding/binary"
    "persistence"
    . "mqtttype"
)

func NewMqttSubscribeCommand() *MqttSubscribeCommand {
    return &MqttSubscribeCommand{
        fixedHeader : MqttFixedHeader{0x20, 0x02}, 
        variableHeader : MqttSubscribeVariableHeader{0},
        payload : MqttSubscribePayload{make([]*MqttSubscribePacket, 0)},
    }
}

type MqttSubscribeCommand struct {
    fixedHeader MqttFixedHeader
    variableHeader MqttSubscribeVariableHeader
    payload MqttSubscribePayload
}

type MqttSubscribeVariableHeader struct {
    packetId int
}

type MqttSubscribePayload struct {
    packets []*MqttSubscribePacket
}

func (cmd *MqttSubscribeCommand) Process(c *Client) error {
    fmt.Println("Process MQTT subscribe command")
    SubscribeInfoSingleton().saveNewSubscribe(c, cmd.payload.packets)
    
    if !c.CleanSession() {
        //save into persistence
        err := persistence.SaveClientSubscribe(c.ClientId(), cmd.payload.packets)
        if err != nil {
            fmt.Println("call SaveClientSubscribe error:", err)
            return err
        }
    }

    //send back Suback command
    ackCmd := NewMqttSubackCommand()
    ackCmd.SetPacketId(cmd.variableHeader.packetId)
    ackCmd.SetReturnCodesLength(len(cmd.payload.packets))
    for index, value := range cmd.payload.packets {
        ackCmd.SetReturnCodes(index, byte(value.Qos))
    }
    c.SendCommand(ackCmd)
    return nil
}

func (cmd *MqttSubscribeCommand) Parse(buf []byte, fixedHeader *MqttFixedHeader) (restBuf []byte, err error) {
    //check the length
    if fixedHeader.remainLength > len(buf) {
        //there is not enough data
        err = &NotCompleteError{"Not a complete subscribe command"}
        return
    }

    cmd.fixedHeader = *fixedHeader

    tempBuf, err := cmd.parseVariableHeader(buf)
    if (err != nil) {
        return
    }
    restBuf, err = cmd.parsePayload(tempBuf)
    if err != nil {
        return
    }
    
    return
}

func (cmd *MqttSubscribeCommand) parseVariableHeader(buf []byte) (restBuf []byte, err error) {
    cmd.variableHeader.packetId = int(binary.BigEndian.Uint16(buf[0:2]))
    fmt.Println("Subscribe command packetId:", cmd.variableHeader.packetId)

    restBuf = buf[2:]
    return
}

func (cmd *MqttSubscribeCommand) parsePayload(buf []byte) (restBuf []byte, err error) {
    index := 0
    
    for index < len(buf) {
        p := new(MqttSubscribePacket)
    
        topicLength := int(binary.BigEndian.Uint16(buf[index:index+2]))
        if index+2+topicLength >= len(buf) {
            err = &ParseError{index, "topic name length is too big"}
            return
        }
        
        p.Topic = string(buf[index+2:index+2+topicLength])
        index += 2+topicLength
        fmt.Println("Subscribe command topic:", p.Topic, ", index:",index,",length:",len(buf))
        
        if index+1 > len(buf) {
            err = &ParseError{index, "there is no qos field"}
            return
        }
        p.Qos = int(buf[index])
        fmt.Println("Subscribe command qos:", p.Qos)
        
        index++
        
        cmd.payload.packets = append(cmd.payload.packets, p)
    }
    
    restBuf = buf[index:]
    return
}

func (cmd *MqttSubscribeCommand) Buffer(buf *bytes.Buffer) error {    
    return nil
}
