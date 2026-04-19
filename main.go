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

	// Track last known phase to detect changes
	var lastPhase config.Phase = config.PhaseLobby
	var lastWinner string = ""

	pn.OnStateChange = func(gs *game.GameState) {
		// Only show full state update if phase changed or game ended
		phaseChanged := gs.Phase != lastPhase
		gameEnded := gs.Winner != "" && lastWinner == ""

		if phaseChanged || gameEnded {
			winner := "TBD"
			if gs.Winner != "" {
				winner = gs.Winner
			}
			fmt.Printf("\n[STATE] Phase=%-6s  Day=%d  Alive=%v  Winner=%s\n",
				gs.Phase, gs.DayNumber, gs.AliveIDs(), winner)

			// Show announcements (eliminations, night results)
			for _, announcement := range gs.Announcements {
				fmt.Printf("  📢 %s\n", announcement)
			}

			// Show phase-specific instructions
			if gs.Winner == "" {
				switch gs.Phase {
				case config.PhaseDay:
					fmt.Printf("\n💬 DAY PHASE: Discuss and vote to eliminate a suspect.\n")
					fmt.Printf("   Commands: speak <msg> | vote <id>\n\n")
				case config.PhaseNight:
					if pn.Role.HasNightAction() {
						fmt.Printf("\n🌙 NIGHT PHASE: Perform your secret action.\n")
						fmt.Printf("   Commands: night <id>\n\n")
					} else {
						fmt.Printf("\n🌙 NIGHT PHASE: Wait while others act in secret...\n\n")
					}
				case config.PhaseLobby:
					fmt.Printf("\n⏳ LOBBY: Waiting for game to start...\n\n")
				}
			} else {
				fmt.Printf("\n🎮 GAME OVER! %s wins!\n", winner)
				fmt.Printf("   Game will reset in 20 seconds...\n\n")
			}

			lastPhase = gs.Phase
			lastWinner = gs.Winner
		} else {
			// Just show announcements for non-phase-change events
			if len(gs.Announcements) > 0 {
				for _, announcement := range gs.Announcements {
					fmt.Printf("  📢 %s\n", announcement)
				}
			}
		}
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
	fmt.Println("\n🎮 DISTRIBUTED MAFIA GAME")
	fmt.Println("════════════════════════════════════════════════════")
	fmt.Println("The game will auto-start once all 5 players connect.")
	fmt.Println("Commands: speak <msg> | vote <id> | night <id> | state | quit")
	fmt.Println("════════════════════════════════════════════════════\n")

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

		default:
			fmt.Println("Unknown command. Try: speak | vote | night | state | quit")
		}
	}
}
