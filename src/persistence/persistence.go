package persistence

import (
    "fmt"
    "log"
    "time"
    "sync"
    
    . "command"
)

type MqttCacheMessage struct {
    Topic string
    Message []byte
    Qos int
    Id int64
}

type persistence interface {
    SaveClientConnection(clientId string, nodeId string, t time.Time) (insertId int64)
    SaveClientSubscribe(clientId string, info []*MqttSubscribePacket)
    RemoveAllSubscribe(clientId string) (topics []string)
    RemoveClientSubscribe(clientId string, topics []string) int64
    GetAllSubscribe(clientId string) (info []*MqttSubscribePacket)
    SaveMessage(clientId string, topic string, msg []byte, packetId int) int64
    SaveClientMessage(clientId string, msgId int64, qos int)
    RemoveClientMessage(clientId string, msgId int64) int64
    GetClientMessage(clientId string) []*MqttCacheMessage
    GetQos2Message(clientId string, packetId int) (id int64, topic string, message []byte)
    UpdateQos2MessageStatus(clientId string, packetId int) int64
}

var p persistence
var once sync.Once

func persistenceSingleton() persistence {
    once.Do(func() {
        p = newMysqlPersistence()
    })
    return p
}

func SaveClientConnection(clientId string, nodeId string, t time.Time) (insertId int64, err error) {
    defer func() {
        // Println executes normally even if there is a panic
        if r := recover(); r != nil {
            var ok bool
            err, ok = r.(error)
            if !ok {
                err = fmt.Errorf("SaveClientConnection failed:%v", r)
            }
        }
    }()
    
    persistenceSingleton().SaveClientConnection(clientId, nodeId, t)
    return
}

func SaveClientSubscribe(clientId string, info []*MqttSubscribePacket) (err error) {
    defer func() {
        // Println executes normally even if there is a panic
        if r := recover(); r != nil {
            var ok bool
            err, ok = r.(error)
            if !ok {
                err = fmt.Errorf("SaveClientSubscribe failed:%v", r)
            }
        }
    }()
    
    persistenceSingleton().SaveClientSubscribe(clientId, info)
    return
}

func RemoveAllSubscribe(clientId string) (topics []string, err error) {
    defer func() {
        // Println executes normally even if there is a panic
        if r := recover(); r != nil {
            var ok bool
            err, ok = r.(error)
            if !ok {
                err = fmt.Errorf("RemoveAllSubscribe failed:%v", r)
            }
        }
    }()
    
    topics = persistenceSingleton().RemoveAllSubscribe(clientId)
    return
}

func RemoveClientSubscribe(clientId string, topics []string) (rows int64, err error) {
    defer func() {
        // Println executes normally even if there is a panic
        if r := recover(); r != nil {
            var ok bool
            err, ok = r.(error)
            if !ok {
                err = fmt.Errorf("RemoveClientSubscribe failed:%v", r)
            }
        }
    }()
    
    rows = persistenceSingleton().RemoveClientSubscribe(clientId, topics)
    return
}

func GetAllSubscribe(clientId string) (info []*MqttSubscribePacket, err error) {
    defer func() {
        // Println executes normally even if there is a panic
        if r := recover(); r != nil {
            var ok bool
            err, ok = r.(error)
            if !ok {
                err = fmt.Errorf("GetAllSubscribe failed:%v", r)
            }
        }
    }()
    
    info = persistenceSingleton().GetAllSubscribe(clientId)
    return
}

func SaveMessage(clientId string, topic string, msg []byte, packetId int) (id int64, err error) {
    defer func() {
        // Println executes normally even if there is a panic
        if r := recover(); r != nil {
            var ok bool
            err, ok = r.(error)
            if !ok {
                err = fmt.Errorf("SaveMessage failed:%v", r)
            }
        }
    }()
    
    id = persistenceSingleton().SaveMessage(clientId, topic, msg, packetId)
    log.Print("SaveMessage id:", id)
    return
}

func SaveClientMessage(clientId string, msgId int64, qos int) (err error) {
    defer func() {
        // Println executes normally even if there is a panic
        if r := recover(); r != nil {
            var ok bool
            err, ok = r.(error)
            if !ok {
                err = fmt.Errorf("SaveClientMessage failed:%v", r)
            }
        }
    }()
    
    persistenceSingleton().SaveClientMessage(clientId, msgId, qos)
    log.Print("SaveClientMessage client[", clientId, "] msgId[", msgId, "] Qos[", qos, "]")
    return
}

func RemoveClientMessage(clientId string, msgId int64) (err error) {
    defer func() {
        // Println executes normally even if there is a panic
        if r := recover(); r != nil {
            var ok bool
            err, ok = r.(error)
            if !ok {
                err = fmt.Errorf("RemoveClientMessage failed:%v", r)
            }
        }
    }()
    
    n := persistenceSingleton().RemoveClientMessage(clientId, msgId)
    log.Print("RemoveClientMessage client[", clientId, "] msgId[", msgId, "] removed", n)
    return
}

func GetClientMessage(clientId string) (msgArray []*MqttCacheMessage, err error) {
    defer func() {
        // Println executes normally even if there is a panic
        if r := recover(); r != nil {
            var ok bool
            err, ok = r.(error)
            if !ok {
                err = fmt.Errorf("GetClientMessage failed:%v", r)
            }
        }
    }()
    
    msgArray = persistenceSingleton().GetClientMessage(clientId)
    log.Print("GetClientMessage client[", clientId, "] message[", len(msgArray), "]")
    return
}

func GetQos2Message(clientId string, packetId int) (id int64, topic string, message []byte, err error) {
    defer func() {
        // Println executes normally even if there is a panic
        if r := recover(); r != nil {
            var ok bool
            err, ok = r.(error)
            if !ok {
                err = fmt.Errorf("GetQos2Message failed:%v", r)
            }
        }
    }()
    
    id, topic, message = persistenceSingleton().GetQos2Message(clientId, packetId)
    return
}

func UpdateQos2MessageStatus(clientId string, packetId int) (rows int64, err error) {
    defer func() {
        // Println executes normally even if there is a panic
        if r := recover(); r != nil {
            var ok bool
            err, ok = r.(error)
            if !ok {
                err = fmt.Errorf("UpdateQos2MessageStatus failed:%v", r)
            }
        }
    }()
    
    rows = persistenceSingleton().UpdateQos2MessageStatus(clientId, packetId)
    return
}
