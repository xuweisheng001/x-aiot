package bridge

import (
	"testing"

	"github.com/xtool/xtool-aiot/internal/pkg/envelope"
)

func TestParseTopic(t *testing.T) {
	cases := []struct {
		name  string
		in    string
		want  Topic
		isErr bool
	}{
		{"telemetry", "up/LM_S1/XT001/telemetry", Topic{"LM_S1", "XT001", envelope.KindTelemetry}, false},
		{"event", "up/LM_S1/XT001/event", Topic{"LM_S1", "XT001", envelope.KindEvent}, false},
		{"cmd_ack", "up/LM_S1/XT001/cmd_ack", Topic{"LM_S1", "XT001", envelope.KindCmdAck}, false},
		{"ota", "up/LM_S1/XT001/ota_progress", Topic{"LM_S1", "XT001", envelope.KindOTAProgress}, false},
		{"shared prefix", "$share/bridge/up/LM_S1/XT001/event", Topic{"LM_S1", "XT001", envelope.KindEvent}, false},
		{"unknown kind", "up/LM_S1/XT001/foo", Topic{}, true},
		{"too short", "up/LM_S1/XT001", Topic{}, true},
		{"too long", "up/LM_S1/XT001/event/extra", Topic{}, true},
		{"wrong root", "down/LM_S1/XT001/event", Topic{}, true},
		{"empty sn", "up/LM_S1//event", Topic{}, true},
		{"wildcard", "up/LM_S1/+/event", Topic{}, true},
		{"share without group", "$share/bridge", Topic{}, true},
		{"empty", "", Topic{}, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := ParseTopic(c.in)
			if (err != nil) != c.isErr {
				t.Fatalf("err=%v want isErr=%v", err, c.isErr)
			}
			if !c.isErr && got != c.want {
				t.Fatalf("got %+v want %+v", got, c.want)
			}
		})
	}
}

func TestExtractSeq(t *testing.T) {
	cases := []struct {
		in   string
		want int64
	}{
		{`{"seq":123,"ts":1}`, 123},
		{`{"ts":1}`, 0},
		{`{"seq":"abc"}`, 0},
		{`{"seq":12.0}`, 12},
		{`not json`, 0},
		{``, 0},
	}
	for _, c := range cases {
		if got := ExtractSeq([]byte(c.in)); got != c.want {
			t.Errorf("ExtractSeq(%q)=%d want %d", c.in, got, c.want)
		}
	}
}
