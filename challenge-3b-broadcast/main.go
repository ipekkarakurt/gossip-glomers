package main

import (
	"encoding/json"
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()
	var messages []float64
	var neighbors []string

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		// Unmarshal the message body as a loosely-typed map.
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		messages = append(messages, body["message"].(float64))

		for _, neighbor := range neighbors {
			n.Send(neighbor, map[string]any{
				"type":    "broadcast",
				"message": body["message"],
			})
		}

		var response = map[string]any{
			"type": "broadcast_ok",
		}

		return n.Reply(msg, response)
	})

	n.Handle("broadcast_ok", func(msg maelstrom.Message) error {
		return nil
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		// Unmarshal the message body as an loosely-typed map.
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		var response = map[string]any{
			"type":     "read_ok",
			"messages": messages,
		}

		return n.Reply(msg, response)
	})

	n.Handle("topology", func(msg maelstrom.Message) error {
		// Unmarshal the message body as an loosely-typed map.
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
