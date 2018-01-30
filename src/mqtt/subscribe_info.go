package mqtt

import (
    "sync"
    "fmt"
    "container/list"
    "reflect"
    . "mqtttype"
)

var info *subscribeInfo
var subOnce sync.Once

func SubscribeInfoSingleton() *subscribeInfo {
    subOnce.Do(func() {
        info = &subscribeInfo{}
        info.subscribeMap = make(map[string]*list.List)
    })
    return info
}

type SubscribePair struct {
    c *Client
    qos int
}

type subscribeInfo struct {
    mutex sync.Mutex
    subscribeMap map[string]*list.List
}

func (m *subscribeInfo) saveNewSubscribe(c *Client, info []*MqttSubscribePacket) {
    m.mutex.Lock()
    defer m.mutex.Unlock()
    for _, value := range info {
        if pairs, ok := m.subscribeMap[value.Topic]; ok {
            var flag bool = false
            
            LOOP:
            for e := pairs.Front(); e != nil; e = e.Next() {
                fmt.Println("iterator subscribe info")
                switch v := e.Value.(type) {
                case *SubscribePair:
                    fmt.Println("iterator subscribe info client[", v.c, "]")
                    if v.c.ClientId() == c.ClientId() {
                        fmt.Println("Modify subscribe client[", c.ClientId(), "] topic[", value.Topic, "] qos[", value.Qos, "]")
                        //exist subscribe info, just modify it
                        v.qos = value.Qos
                        flag = true
                        break LOOP
                    }
                default:
                    fmt.Println("Wrong type:", reflect.TypeOf(e.Value))
                }
            }
            
            if !flag {
                //add new subscribe info here
                m.subscribeMap[value.Topic].PushBack(&SubscribePair{c, value.Qos})
                fmt.Println("Save subscribe client[", c.ClientId(), "] topic[", value.Topic, "] qos[", value.Qos, "]")
            }
        } else {
            m.subscribeMap[value.Topic] = list.New()
            m.subscribeMap[value.Topic].PushBack(&SubscribePair{c, value.Qos})
            fmt.Println("Save subscribe client[", c.ClientId(), "] topic[", value.Topic, "] qos[", value.Qos, "]")
        }
    }
}

func (m *subscribeInfo) getSubscribedClients(topic string) []SubscribePair {
    m.mutex.Lock()
    defer m.mutex.Unlock()
    index := 0
    l := m.subscribeMap[topic]
    
    if l == nil {
        //there is no client subscribed that topic
        return nil
    }
    
    var pairs []SubscribePair = make([]SubscribePair, l.Len())
    for e := l.Front(); e != nil; e = e.Next() {
        switch v := e.Value.(type) {
        case *SubscribePair:
            pairs[index] = *v
        default:
            fmt.Println("Wrong SubscribePair type:", e.Value)
        }
        index++
	}
    return pairs
}

func (m *subscribeInfo) removeSubscribe(c *Client, info []*MqttSubscribePacket) {
    m.mutex.Lock()
    defer m.mutex.Unlock()
    for _, value := range info {
        if pairs, ok := m.subscribeMap[value.Topic]; ok {
            var flag bool = false
            for e := pairs.Front(); e != nil; e = e.Next() {
                pair, typeOk := e.Value.(SubscribePair)
                if !typeOk {
                    fmt.Println("Wrong SubscribePair type:", e.Value)
                    continue
                }

                if pair.c == c {
                    if value.Qos >= pair.qos {
                        //remove subscribe info
                        pairs.Remove(e)
                    }
                    flag = true
                    break
                }
            }
            
            if !flag {
                fmt.Println("Client[", c.ClientId(), "] don't subscribe topic[", value.Topic, "]")
            }
        } else {
            fmt.Println("Client[", c.ClientId(), "] don't subscribe topic[", value.Topic, "]")
        }
    }
}
