package impl

import (
    "log"
    . "command"
    . "client"
)

func NewMqttCommand(cmdType int) MqttCommand {
    switch cmdType {
    case MQTT_CMD_SUBSCRIBE:
        return new(MqttSubscribeCommand)
    case MQTT_CMD_UNSUBSCRIBE:
        return NewMqttUnSubscribeCommand()
    case MQTT_CMD_PINGREQ:
        return new(MqttPingReqCommand)
    case MQTT_CMD_PUBLISH:
        return NewMqttPublishCommand()
    case MQTT_CMD_PUBACK:
        return NewMqttPubackCommand()
    case MQTT_CMD_PUBREC:
        return NewMqttPubRecCommand()
    case MQTT_CMD_PUBREL:
        return NewMqttPubRelCommand()
    case MQTT_CMD_PUBCOMP:
        return NewMqttPubCompCommand()
    default:
        log.Print("Error command type", cmdType)
        return nil
    }
}