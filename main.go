// main.go — Entry point for a single player node.
//
// Usage:
//
//	go run . <node_id>
//
//	node_id must be 0-4. Each player runs this on their own machine
//	(or in a separate terminal for local testing).
//
// Example — open 5 terminals:
//
//	go run . 0   # Mafia
//	go run . 1   # Civilian
//	go run . 2   # Civilian
//	go run . 3   # Civilian
//	go run . 4   # Doctor
package main

import (
	"bufio"
	"fmt"
	"log"
	"mafia-p2p/config"
	"mafia-p2p/game"
	"mafia-p2p/node"
	"os"
	"strconv"
	"strings"
	"time"
)

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "Usage: go run . <node_id>  (node_id: 0-4)")
		os.Exit(1)
	}
	nodeID, err := strconv.Atoi(os.Args[1])
	if err != nil || nodeID < 0 || nodeID >= config.NumNodes {
		fmt.Fprintf(os.Stderr, "node_id must be an integer 0-%d\n", config.NumNodes-1)
		os.Exit(1)
	}

	pn := node.NewPlayerNode(nodeID, config.DefaultRoleMap)

	// ── UI callbacks ──────────────────────────────────────────────────────────
	pn.OnStateChange = func(gs *game.GameState) {
		winner := "TBD"
		if gs.Winner != "" {
			winner = gs.Winner
		}
		fmt.Printf("\n[STATE] Phase=%-6s  Day=%d  Alive=%v  Winner=%s\n",
			gs.Phase, gs.DayNumber, gs.AliveIDs(), winner)
	}

	pn.OnMessage = func(senderID int, text string) {
		fmt.Printf("  [CHAT] Player %d: %s\n", senderID, text)
	}

	if err := pn.Start(); err != nil {
		log.Fatalf("Failed to start node: %v", err)
	}
	defer pn.Stop()

	fmt.Printf("Node %d starting — waiting 5 s for peers to connect…\n", nodeID)
	time.Sleep(5 * time.Second)

	fmt.Println(pn.DumpState())
	fmt.Println("\nCommands: speak <msg> | vote <id> | night <id> | phase <DAY|NIGHT> | state | quit")

	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Print("> ")
		if !scanner.Scan() {
			break
		}
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		parts := strings.SplitN(line, " ", 2)
		cmd := strings.ToLower(parts[0])
		arg := ""
		if len(parts) > 1 {
			arg = parts[1]
		}

		switch cmd {
		case "quit", "exit":
			return

		case "state":
			fmt.Println(pn.DumpState())

		case "speak":
			if arg == "" {
				fmt.Println("Usage: speak <message>")
				continue
			}
			if pn.Speak(arg) {
				fmt.Println("✓ Spoke")
			} else {
				fmt.Println("✗ Could not speak")
			}

		case "vote":
			id, err := strconv.Atoi(arg)
			if err != nil {
				fmt.Println("Usage: vote <node_id>")
				continue
			}
			if pn.Vote(id) {
				fmt.Println("✓ Vote cast")
			} else {
				fmt.Println("✗ Vote failed")
			}

		case "night":
			id, err := strconv.Atoi(arg)
			if err != nil {
				fmt.Println("Usage: night <node_id>")
				continue
			}
			if pn.PerformNightAction(id) {
				fmt.Println("✓ Night action performed")
			} else {
				fmt.Println("✗ Night action failed")
			}

		case "phase":
			phase, ok := config.PhaseFromString(strings.ToUpper(arg))
			if !ok {
				fmt.Println("Usage: phase <DAY|NIGHT>")
				continue
			}
			if pn.ProposePhaseChange(phase) {
				fmt.Printf("✓ Phase change to %s proposed\n", phase)
			} else {
				fmt.Println("✗ Phase change failed")
			}

		default:
			fmt.Println("Unknown command. Try: speak | vote | night | phase | state | quit")
		}
	}
}
