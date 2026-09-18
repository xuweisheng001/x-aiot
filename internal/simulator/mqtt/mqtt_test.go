package mqtt

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"net"
	"sync"
	"testing"
	"time"
)

func TestRemainingLength(t *testing.T) {
	tests := map[int][]byte{0: {0}, 127: {127}, 128: {0x80, 1}, 16383: {0xff, 0x7f}, 16384: {0x80, 0x80, 1}}
	for n, want := range tests {
		if got := encodeRemainingLength(n); !bytes.Equal(got, want) {
			t.Errorf("%d: %v want %v", n, got, want)
		}
		p, err := readPacket(bufio.NewReader(bytes.NewReader(append(frame(typePUBLISH, 0, nil)[:1], append(want, make([]byte, n)...)...))))
		if err != nil || len(p.body) != n {
			t.Errorf("roundtrip %d: %v len %d", n, err, len(p.body))
		}
	}
}

func TestPublishRoundtrip(t *testing.T) {
	tests := []struct {
		topic   string
		qos     byte
		pid     uint16
		payload string
	}{
		{"up/LM_S1/SIM00001/telemetry", 0, 0, `{"seq":1}`},
		{"down/SIM00001/cmd", 1, 42, `{"cmd_id":"x"}`},
		{"t", 1, 65535, ""},
	}
	for _, tc := range tests {
		b := publishPacket(tc.topic, tc.qos, tc.pid, []byte(tc.payload))
		p, err := readPacket(bufio.NewReader(bytes.NewReader(b)))
		if err != nil {
			t.Fatal(err)
		}
		m, err := parsePublish(p)
		if err != nil {
			t.Fatal(err)
		}
		if m.Topic != tc.topic || m.QoS != tc.qos || m.PID != tc.pid || string(m.Payload) != tc.payload {
			t.Fatalf("got %+v want %+v", m, tc)
		}
	}
}

func TestConnectPacketFlags(t *testing.T) {
	p, _ := readPacket(bufio.NewReader(bytes.NewReader(connectPacket("SIM00001", "SIM00001", "", false, 60))))
	if p.typ != typeCONNECT {
		t.Fatal("type")
	}
	// "MQTT"(6) + level(1) + flags(1) + keepalive(2)
	flags := p.body[7]
	if flags&0x02 != 0 {
		t.Fatal("cleanSession must be off")
	}
	if flags&0x80 == 0 || flags&0x40 != 0 {
		t.Fatal("username on, password off")
	}
	if binary.BigEndian.Uint16(p.body[8:10]) != 60 {
		t.Fatal("keepalive")
	}
	cid, _, _ := readString(p.body[10:])
	if cid != "SIM00001" {
		t.Fatal(cid)
	}
}

func TestClientAgainstFakeBroker(t *testing.T) {
	br, err := NewFakeBroker()
	if err != nil {
		t.Fatal(err)
	}
	defer br.Close()
	got := make(chan Message, 1)
	var acks int
	var amu sync.Mutex
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cli, err := Dial(ctx, Options{Addr: br.Addr(), ClientID: "SIM00001", KeepAlive: 200 * time.Millisecond,
		OnMessage: func(m Message) { got <- m },
		OnPubAck:  func() { amu.Lock(); acks++; amu.Unlock() },
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := cli.Subscribe(ctx, "down/SIM00001/#", 1); err != nil {
		t.Fatal(err)
	}
	br.Push("down/SIM00001/cmd", []byte(`{"cmd_id":"c1","action":"pause"}`))
	select {
	case m := <-got:
		if m.Topic != "down/SIM00001/cmd" || m.QoS != 1 {
			t.Fatalf("unexpected %+v", m)
		}
	case <-ctx.Done():
		t.Fatal("no downlink message")
	}
	if err := cli.Publish("up/LM_S1/SIM00001/telemetry", 1, []byte(`{"seq":1}`)); err != nil {
		t.Fatal(err)
	}
	if err := cli.Publish("up/LM_S1/SIM00001/event", 0, []byte(`{"seq":2}`)); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		amu.Lock()
		a := acks
		amu.Unlock()
		if len(br.Received()) == 2 && a == 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if len(br.Received()) != 2 {
		t.Fatalf("broker got %d publishes", len(br.Received()))
	}
	// keepalive pings survive a while
	time.Sleep(500 * time.Millisecond)
	if cli.Err() != nil {
		t.Fatalf("connection dropped: %v", cli.Err())
	}
	cli.Disconnect()
	select {
	case <-cli.Done():
	case <-time.After(time.Second):
		t.Fatal("Done not closed")
	}
	if err := cli.Publish("x", 0, nil); err == nil {
		t.Fatal("publish after close must fail")
	}
}

func TestDialRefusedConnack(t *testing.T) {
	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	defer ln.Close()
	go func() {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		defer c.Close()
		_, _ = readPacket(bufio.NewReader(c))
		c.Write(frame(typeCONNACK, 0, []byte{0, 5})) // not authorized
	}()
	_, err := Dial(context.Background(), Options{Addr: ln.Addr().String(), ClientID: "X"})
	if err == nil {
		t.Fatal("expected refusal")
	}
}
