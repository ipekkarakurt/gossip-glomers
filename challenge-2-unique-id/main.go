package main

import (
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type IDGenerator struct {
	counter int
	mutex   sync.Mutex
	nodeId  int
}

func NewIDGenerator(nodeId int) *IDGenerator {
	return &IDGenerator{
		nodeId: nodeId,
	}
}

func (g *IDGenerator) GenerateID() string {
	g.mutex.Lock()
	defer g.mutex.Unlock()
	g.counter++

	// ID generation is loosely inspired by Twitter's Snowflake ID generation - timestamp + node id + counter value specific to that node
	id := fmt.Sprintf("%d%d%d", time.Now().UnixNano(), g.nodeId, g.counter)
	return id
}

func main() {
	n := maelstrom.NewNode()

	var nodeId, _ = strconv.ParseInt(n.ID(), 10, 64)
	generator := NewIDGenerator(int(nodeId))

	n.Handle("generate", func(msg maelstrom.Message) error {
		// Unmarshal the message body as an loosely-typed map.
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		// Update the message type
		body["type"] = "generate_ok"
		// Generate unique id
		id := generator.GenerateID()
		body["id"] = id

		// Echo the original message back with the updated type and id.
		return n.Reply(msg, body)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
