package mqtt

import (
    "errors"
    "fmt"
)

type MqttCommand interface {
    //GetFixedHeader() MqttFixedHeader
    Process() error
}

type MqttFixedHeader struct {
    flag byte
    remainLength int
}

func (fixedHeader *MqttFixedHeader) Parse(buf []byte) (restBuf []byte, err error) {
    if len(buf) < 2 {
        err = errors.New("Not a complete commmand")
        return
    }
    
    flag := buf[0]
    
    index := 1
    multiplier := 1
    value := 0
    for {
        encodedByte := buf[index]
        index++
        value += int(encodedByte & 127) * multiplier
        multiplier *= 128
        
        if multiplier > 128*128*128 {
            err = errors.New("remain length format error")
            return
        }
        
        if encodedByte & 128 == 0 {
            break
        }

        if index >= len(buf) {
            err = errors.New("Not a complete commmand")
            return
        }
    }
    
    fixedHeader.flag = flag
    fixedHeader.remainLength = value
    
    restBuf = buf[index:]
    fmt.Println("Type:", fixedHeader.GetPacketType(), ", remainlength:", fixedHeader.GetRemainLength(), ", index:", index)
    return
}

func (fixedHeader *MqttFixedHeader) GetPacketType() int {
    return int(fixedHeader.flag >> 4)
}

func (fixedHeader *MqttFixedHeader) GetFlagDup() bool {
    if fixedHeader.flag & 0x08 == 0x08 {
        return true
    } else {
        return false
    }
}

func (fixedHeader *MqttFixedHeader) GetFlagQos() int {
    return int((fixedHeader.flag & 0x06) >> 1)
}

func (fixedHeader *MqttFixedHeader) GetFlagRetain() bool {
    if fixedHeader.flag & 0x01 == 0x01 {
        return true
    } else {
        return false
    }
}

func (fixedHeader *MqttFixedHeader) GetRemainLength() int {
    return fixedHeader.remainLength
}
