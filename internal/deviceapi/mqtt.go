package deviceapi

import (
	"fmt"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

// MQTTPublisher 是下行发布抽象（QoS1）。
type MQTTPublisher interface {
	Publish(topic string, payload []byte) error
}

// PahoPublisher 用 paho 客户端实现 MQTTPublisher。
type PahoPublisher struct {
	Client  mqtt.Client
	Timeout time.Duration
}

func (p *PahoPublisher) Publish(topic string, payload []byte) error {
	tok := p.Client.Publish(topic, 1, false, payload)
	if !tok.WaitTimeout(p.Timeout) {
		return fmt.Errorf("mqtt publish %s: timeout", topic)
	}
	if err := tok.Error(); err != nil {
		return fmt.Errorf("mqtt publish %s: %w", topic, err)
	}
	return nil
}

// TopicCmd / TopicDesired 是云 → 设备 topic。
func TopicCmd(sn string) string     { return "down/" + sn + "/cmd" }
func TopicDesired(sn string) string { return "down/" + sn + "/desired" }
