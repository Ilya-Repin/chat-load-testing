package main

import (
	"bytes"
	"context"
	"encoding/json"
	"expvar"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"strconv"
	"time"

	"github.com/spf13/afero"
	"github.com/yandex/pandora/cli"
	"github.com/yandex/pandora/core"
	"github.com/yandex/pandora/core/aggregator/netsample"
	coreimport "github.com/yandex/pandora/core/import"
	"github.com/yandex/pandora/core/register"
	"go.uber.org/zap"
)

//
// ===== Config =====
//

type GunConfig struct {
	Target  string `validate:"required"`
	Prefix  string `validate:"required"`
	Pollers int    `validate:"min=1"`
	Delay   int    `validate:"min=1"`

	RegisterPath  string
	PollStartPath string
	PollPath      string
	SendPath      string
}

//
// ===== Models =====
//

type User struct {
	Username  string
	Token     string
	SessionID string
}

var deliveryLatencyMs = expvar.NewInt("delivery_latency_ms")

//
// ===== Gun =====
//

type Gun struct {
	conf GunConfig
	aggr core.Aggregator
	core.GunDeps

	client *http.Client

	self User

	ctx  context.Context
	stop context.CancelFunc
}

func NewGun(conf GunConfig) *Gun {
	return &Gun{conf: conf}
}

func (g *Gun) Bind(aggr core.Aggregator, deps core.GunDeps) error {
	g.aggr = aggr
	g.GunDeps = deps

	if g.conf.Delay > 0 {
		sleepTime := time.Duration(g.GunDeps.InstanceID*g.conf.Delay) * time.Millisecond

		time.Sleep(sleepTime)
	}

	g.client = &http.Client{
		Transport: &http.Transport{
			MaxIdleConns:        90000,
			MaxIdleConnsPerHost: 90000,
			MaxConnsPerHost:     90000,
		},
		Timeout: 0,
	}

	g.ctx, g.stop = context.WithCancel(context.Background())

	user, err := g.registerUser(deps.InstanceID)
	if err != nil {
		return err
	}
	g.self = user

	if g.GunDeps.InstanceID < g.conf.Pollers {
		sid, err := g.pollStart(g.self)
		if err != nil {
			g.Log.Error("failed to start poll", zap.Error(err))

			return err
		}

		g.self.SessionID = sid

		go g.pollLoop()
	}

	return nil
}

//
// ===== Shoot =====
//

func (g *Gun) Shoot(ammo core.Ammo) {
	receiver := g.randomPeer()
	ts := time.Now().UnixMilli()

	body := map[string]any{
		"message": map[string]any{
			"recipient": receiver,
			"payload":   strconv.FormatInt(ts, 10),
		},
	}

	data, _ := json.Marshal(body)

	req, _ := http.NewRequest(
		"POST",
		g.conf.Target+g.conf.SendPath,
		bytes.NewReader(data),
	)
	req.Header.Set("Authorization", "Bearer "+g.self.Token)
	req.Header.Set("Content-Type", "application/json")

	sample := netsample.Acquire("send")
	resp, err := g.client.Do(req)
	defer g.aggr.Report(sample)

	if err != nil {
		sample.SetProtoCode(0)
		return
	}
	sample.SetProtoCode(resp.StatusCode)

	defer resp.Body.Close()
}

//
// ===== Polling =====
//

func (g *Gun) pollLoop() {
	for {
		randomMillis := rand.Intn(101-10) + 10
		pauseDuration := time.Duration(randomMillis) * time.Millisecond

		select {
		case <-g.ctx.Done():
			return
		case <-time.After(pauseDuration):
		}

		func() {
			req, _ := http.NewRequest(
				"GET",
				g.conf.Target+g.conf.PollPath+"/"+g.self.SessionID,
				nil,
			)
			req.Header.Set("Authorization", "Bearer "+g.self.Token)

			sample := netsample.Acquire("poll")
			defer g.aggr.Report(sample)

			resp, err := g.client.Do(req)
			if err != nil {
				g.Log.Error("failed to poll", zap.Error(err))
				sample.SetProtoCode(0)

				return
			}
			defer resp.Body.Close()

			sample.SetProtoCode(resp.StatusCode)
			if resp.StatusCode == http.StatusOK {
				g.handleMessages(resp.Body)
			}
		}()
	}
}

func (g *Gun) handleMessages(r io.Reader) {
	body, err := io.ReadAll(r)
	if err != nil {
		g.Log.Error("failed to read body", zap.Error(err))
		return
	}

	var out struct {
		Messages []struct {
			Text string `json:"text"`
		} `json:"messages"`
	}

	if err := json.Unmarshal(body, &out); err != nil {
		g.Log.Debug("failed to unmarshal json", zap.Error(err))
		return
	}

	now := time.Now()
	for _, m := range out.Messages {
		ts, err := strconv.ParseInt(m.Text, 10, 64)
		if err != nil {
			continue
		}

		e2e := now.Sub(time.UnixMilli(ts)).Milliseconds()
		deliveryLatencyMs.Add(e2e)
	}
}

//
// ===== Helpers =====
//

func (g *Gun) randomPeer() string {
	for {
		id := rand.Intn(g.conf.Pollers)
		if id != g.InstanceID {
			return g.conf.Prefix + strconv.Itoa(id)
		}
	}
}

func (g *Gun) registerUser(instance int) (User, error) {
	username := g.conf.Prefix + strconv.Itoa(instance)

	body := map[string]any{
		"user": map[string]any{
			"username":     username,
			"password":     "PassWord_" + strconv.Itoa(instance),
			"display_name": username,
			"biography":    "pandora test user",
		},
	}

	data, _ := json.Marshal(body)

	resp, err := g.client.Post(
		g.conf.Target+g.conf.RegisterPath,
		"application/json",
		bytes.NewReader(data),
	)
	if err != nil {
		return User{}, err
	}
	defer resp.Body.Close()
	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return User{}, fmt.Errorf("failed to read response body: %w", err)
	}
	var out struct {
		User struct {
			Username string `json:"username"`
			Token    string `json:"token"`
		} `json:"user"`
	}

	if err := json.Unmarshal(bodyBytes, &out); err != nil {
		return User{}, fmt.Errorf("failed to parse response: %w", err)
	}

	return User{
		Username: out.User.Username,
		Token:    out.User.Token,
	}, nil
}

func (g *Gun) pollStart(u User) (string, error) {
	req, _ := http.NewRequest(
		"POST",
		g.conf.Target+g.conf.PollStartPath,
		nil,
	)
	req.Header.Set("Authorization", "Bearer "+u.Token)

	resp, err := g.client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	var out struct {
		SessionID string `json:"session_id"`
	}
	_ = json.NewDecoder(resp.Body).Decode(&out)

	return out.SessionID, nil
}

//
// ===== main =====
//

func main() {
	rand.Seed(time.Now().UnixNano())

	fs := afero.NewOsFs()
	coreimport.Import(fs)

	register.Gun("chat-gun", NewGun, func() GunConfig {
		return GunConfig{
			RegisterPath:  "/v1/users/register",
			PollStartPath: "/v1/messages/poll/start",
			PollPath:      "/v1/messages/poll",
			SendPath:      "/v1/messages/send",
		}
	})

	cli.Run()
}
