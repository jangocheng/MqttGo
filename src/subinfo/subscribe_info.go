package subinfo

import (
    "sync"
    "log"
    "container/list"
    "reflect"
    
    . "command"
    . "client"
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
    C Client
    Qos int
}

type subscribeInfo struct {
    mutex sync.Mutex
    subscribeMap map[string]*list.List
}

func (m *subscribeInfo) SaveNewSubscribe(c Client, info []*MqttSubscribePacket) {
    m.mutex.Lock()
    defer m.mutex.Unlock()
    for _, value := range info {
        log.Print("saveNewSubscribe process Topic[", value.Topic, "] Qos[", value.Qos, "]")
        if pairs, ok := m.subscribeMap[value.Topic]; ok {
            var flag bool = false
            
            LOOP:
            for e := pairs.Front(); e != nil; e = e.Next() {
                switch v := e.Value.(type) {
                case *SubscribePair:
                    log.Print("iterator subscribe info client[", v.C, "]")
                    if v.C.ClientId() == c.ClientId() {
                        log.Print("Modify subscribe client[", c.ClientId(), "] topic[", value.Topic, "] qos[", value.Qos, "]")
                        //exist subscribe info, just modify it
                        v.Qos = value.Qos
                        flag = true
                        break LOOP
                    }
                default:
                    log.Print("Wrong type:", reflect.TypeOf(e.Value))
                }
            }
            
            if !flag {
                //add new subscribe info here
                m.subscribeMap[value.Topic].PushBack(&SubscribePair{c, value.Qos})
                log.Print("Save subscribe client[", c.ClientId(), "] topic[", value.Topic, "] qos[", value.Qos, "]")
            }
        } else {
            m.subscribeMap[value.Topic] = list.New()
            m.subscribeMap[value.Topic].PushBack(&SubscribePair{c, value.Qos})
            log.Print("Save subscribe client[", c.ClientId(), "] topic[", value.Topic, "] qos[", value.Qos, "]")
        }
    }
}

func (m *subscribeInfo) GetSubscribedClients(topic string) []SubscribePair {
    m.mutex.Lock()
    defer m.mutex.Unlock()
    index := 0
    l := m.subscribeMap[topic]
    
    if l == nil {
        //there is no client subscribed that topic
        log.Print("getSubscribedClients there is no client subscribe topic[", topic, "]")
        return nil
    }
    
    var pairs []SubscribePair = make([]SubscribePair, l.Len())
    for e := l.Front(); e != nil; e = e.Next() {
        switch v := e.Value.(type) {
        case *SubscribePair:
            pairs[index] = *v
        default:
            log.Print("Wrong SubscribePair type:", e.Value)
        }
        index++
	}
    return pairs
}

func (m *subscribeInfo) RemoveSubscribe(c Client, topics []string) {
    m.mutex.Lock()
    defer m.mutex.Unlock()
    for _, topic := range topics {
        if pairs, ok := m.subscribeMap[topic]; ok {
            var flag bool = false
            for e := pairs.Front(); e != nil; e = e.Next() {
                pair, typeOk := e.Value.(SubscribePair)
                if !typeOk {
                    log.Print("Wrong SubscribePair type:", e.Value)
                    continue
                }

                if pair.C == c {
                    //remove subscribe info
                    pairs.Remove(e)
                    flag = true
                    break
                }
            }
            
            if !flag {
                log.Print("Client[", c.ClientId(), "] don't subscribe topic[", topic, "]")
            }
        } else {
            log.Print("Client[", c.ClientId(), "] don't subscribe topic[", topic, "]")
        }
    }
}
