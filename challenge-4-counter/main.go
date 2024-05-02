package main

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

const defaultTimeout = time.Second

func main() {
	n := maelstrom.NewNode()
	kv := maelstrom.NewSeqKV(n)
	var mutex sync.Mutex

	n.Handle("init", func(msg maelstrom.Message) error {
		ctx, _ := context.WithTimeout(context.Background(), defaultTimeout)

		mutex.Lock()
		defer mutex.Unlock()
		if err := kv.Write(ctx, n.ID(), 0); err != nil {
			for err != nil {
				err = kv.Write(ctx, n.ID(), 0)
			}
		}
		return nil
	})

	n.Handle("add", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		delta := int(body["delta"].(float64))

		ctx, _ := context.WithTimeout(context.Background(), defaultTimeout)

		mutex.Lock()
		defer mutex.Unlock()
		sum, err := kv.ReadInt(ctx, n.ID())
		if err != nil {
			for err != nil {
				sum, err = kv.ReadInt(ctx, n.ID())
			}
		}

		ctx, _ = context.WithTimeout(context.Background(), defaultTimeout)

		err = kv.Write(ctx, n.ID(), sum+delta)
		if err != nil {
			for err != nil {
				err = kv.Write(ctx, n.ID(), sum+delta)
			}
		}

		return n.Reply(msg, map[string]any{
			"type": "add_ok",
		})
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		sum := 0
		ctx, _ := context.WithTimeout(context.Background(), defaultTimeout)

		mutex.Lock()
		defer mutex.Unlock()
		v, _ := kv.ReadInt(ctx, n.ID())

		sum += v

		return n.Reply(msg, map[string]any{
			"type":  "read_ok",
			"value": sum,
		})
	})

	n.Handle("local", func(msg maelstrom.Message) error {
		ctx, _ := context.WithTimeout(context.Background(), defaultTimeout)

		mutex.Lock()
		defer mutex.Unlock()
		v, _ := kv.ReadInt(ctx, n.ID())

		return n.Reply(msg, map[string]any{
			"type":  "local_ok",
			"value": v,
		})
	})
}
