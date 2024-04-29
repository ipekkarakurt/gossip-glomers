package main

import (
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/lrita/cmap"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()
	var messages cmap.Cmap
	var neighbors []string

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		var body struct {
			Message   int `json:"message"`
			MessageID int `json:"msg_id"`
		}

		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		if _, ok := messages.Load(body.Message); ok {
			return nil
		}

		messages.Store(body.Message, true)

		for _, neighbor := range neighbors {
			acked := false
			tries := 0
			for !acked {
				tries++
				n.RPC(neighbor, map[string]any{
					"type":    "broadcast",
					"message": body.Message,
				},
					func(msg maelstrom.Message) error {
						acked = true
						return nil
					})
				log.Println(fmt.Sprintf("Node %v: Message to Node %v not acked, try #%v", n.ID(), neighbor, tries))
			}
			time.Sleep(100 * time.Millisecond)

		}

		return n.Reply(msg, map[string]any{
			"type": "broadcast_ok",
		})
	})

	n.Handle("broadcast_ok", func(msg maelstrom.Message) error {
		return nil
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		keys := make([]int, 0, messages.Count())
		messages.Range(func(key, value interface{}) bool {
			keys = append(keys, key.(int))
			return true
		})

		var response = map[string]any{
			"type":     "read_ok",
			"messages": keys,
		}

		return n.Reply(msg, response)
	})

	n.Handle("topology", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		topology, _ := body["topology"].(map[string]interface{})

		for node, nodeNeighborsList := range topology {
			if node == n.ID() {
				neighborsListInterface, _ := nodeNeighborsList.([]interface{})
				for _, neighbor := range neighborsListInterface {
					neighbors = append(neighbors, neighbor.(string))
				}
			}
		}
		var response = map[string]any{
			"type": "topology_ok",
		}

		return n.Reply(msg, response)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
