package client

import (
    "bytes"
    
    . "command"
)

type MqttCommand interface {
    Process(c Client) error
    Parse(buf []byte, fixedHeader *MqttFixedHeader) (restBuf []byte, err error)
    Buffer(buf *bytes.Buffer) error
}

type Client interface {
    ClientId() string
    Username() string
    CleanSession() bool
    NextPacketId() uint16
    SavePacketIdMapping(packetId uint16, msgId int64)
    RemovePacketIdMapping(packetId uint16) (int64, error)
    SendCommand(cmd MqttCommand) error
    ProcessInputCommand()
    Close()
    Read()
}