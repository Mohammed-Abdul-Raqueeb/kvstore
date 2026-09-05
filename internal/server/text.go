package server

import (
	"bufio"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"
)

// The text protocol is a debugging aid, not a second product (DESIGN.md §4).
//
// It exists so you can `telnet` or `nc` into a running server and poke at it
// without a client binary, which is worth a great deal at 2am. It is kept
// strictly separate from the binary path: it does not share the frame
// decoder, it does not go near the worker pool, and it is disabled unless
// --text-addr is set. Its parsing is deliberately simple-minded because
// nothing about it is on a hot path.
//
// Limitation, stated rather than hidden: keys and values here are
// whitespace-delimited tokens, so this interface cannot express keys
// containing spaces, newlines or NUL bytes — all of which the binary
// protocol handles fine. Use kvctl for those.

func (s *Server) acceptTextLoop(ln net.Listener) {
	defer s.wg.Done()
	for {
		nc, err := ln.Accept()
		if err != nil {
			if s.shutdown.Load() {
				return
			}
			return
		}
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			s.serveText(nc)
		}()
	}
}

func (s *Server) serveText(nc net.Conn) {
	defer nc.Close()
	r := bufio.NewReader(nc)
	w := bufio.NewWriter(nc)
	defer w.Flush()

	fmt.Fprintf(w, "+kvstore text debug interface; type HELP or QUIT\r\n")
	w.Flush()

	for {
		if s.cfg.IdleTimeout > 0 {
			_ = nc.SetReadDeadline(time.Now().Add(s.cfg.IdleTimeout))
		}
		line, err := r.ReadString('\n')
		if err != nil {
			return
		}
		fields := strings.Fields(strings.TrimRight(line, "\r\n"))
		if len(fields) == 0 {
			continue
		}
		cmd := strings.ToUpper(fields[0])
		args := fields[1:]

		switch cmd {
		case "QUIT", "EXIT":
			fmt.Fprintf(w, "+BYE\r\n")
			w.Flush()
			return

		case "HELP":
			fmt.Fprintf(w, "%s", textHelp)

		case "PING":
			fmt.Fprintf(w, "+PONG\r\n")

		case "GET":
			if len(args) != 1 {
				fmt.Fprintf(w, "-ERR usage: GET <key>\r\n")
				break
			}
			v, ok := s.eng.Get([]byte(args[0]))
			if !ok {
				fmt.Fprintf(w, "$-1\r\n")
			} else {
				fmt.Fprintf(w, "$%d\r\n%s\r\n", len(v), v)
			}

		case "SET":
			if len(args) < 2 || len(args) > 3 {
				fmt.Fprintf(w, "-ERR usage: SET <key> <value> [ttl_ms]\r\n")
				break
			}
			var ttl uint64
			if len(args) == 3 {
				n, err := strconv.ParseUint(args[2], 10, 64)
				if err != nil {
					fmt.Fprintf(w, "-ERR bad ttl: %v\r\n", err)
					break
				}
				ttl = n
			}
			if err := s.eng.Set([]byte(args[0]), []byte(args[1]), ttl); err != nil {
				fmt.Fprintf(w, "-ERR %v\r\n", err)
			} else {
				fmt.Fprintf(w, "+OK\r\n")
			}

		case "DEL", "DELETE":
			if len(args) != 1 {
				fmt.Fprintf(w, "-ERR usage: DEL <key>\r\n")
				break
			}
			existed, err := s.eng.Delete([]byte(args[0]))
			if err != nil {
				fmt.Fprintf(w, "-ERR %v\r\n", err)
			} else if existed {
				fmt.Fprintf(w, ":1\r\n")
			} else {
				fmt.Fprintf(w, ":0\r\n")
			}

		case "EXISTS":
			if len(args) != 1 {
				fmt.Fprintf(w, "-ERR usage: EXISTS <key>\r\n")
				break
			}
			if s.eng.Exists([]byte(args[0])) {
				fmt.Fprintf(w, ":1\r\n")
			} else {
				fmt.Fprintf(w, ":0\r\n")
			}

		case "TTL":
			if len(args) != 1 {
				fmt.Fprintf(w, "-ERR usage: TTL <key>\r\n")
				break
			}
			ms, ok := s.eng.TTL([]byte(args[0]))
			if !ok {
				fmt.Fprintf(w, ":-2\r\n")
			} else {
				fmt.Fprintf(w, ":%d\r\n", ms)
			}

		case "EXPIRE":
			if len(args) != 2 {
				fmt.Fprintf(w, "-ERR usage: EXPIRE <key> <ttl_ms>\r\n")
				break
			}
			ttl, err := strconv.ParseUint(args[1], 10, 64)
			if err != nil {
				fmt.Fprintf(w, "-ERR bad ttl: %v\r\n", err)
				break
			}
			ok, err := s.eng.Expire([]byte(args[0]), ttl)
			if err != nil {
				fmt.Fprintf(w, "-ERR %v\r\n", err)
			} else if ok {
				fmt.Fprintf(w, ":1\r\n")
			} else {
				fmt.Fprintf(w, ":0\r\n")
			}

		case "KEYS":
			prefix := ""
			if len(args) > 0 {
				prefix = args[0]
			}
			keys := s.eng.Keys([]byte(prefix), 100)
			fmt.Fprintf(w, "*%d\r\n", len(keys))
			for _, k := range keys {
				fmt.Fprintf(w, "$%d\r\n%s\r\n", len(k), k)
			}

		case "STATS":
			b, err := s.statsJSON()
			if err != nil {
				fmt.Fprintf(w, "-ERR %v\r\n", err)
			} else {
				fmt.Fprintf(w, "$%d\r\n%s\r\n", len(b), b)
			}

		case "SNAPSHOT":
			res, err := s.eng.Snapshot()
			if err != nil {
				fmt.Fprintf(w, "-ERR %v\r\n", err)
			} else {
				fmt.Fprintf(w, "+OK %s (%d entries)\r\n", res.Path, res.Entries)
			}

		default:
			fmt.Fprintf(w, "-ERR unknown command %q\r\n", cmd)
		}
		if err := w.Flush(); err != nil {
			return
		}
	}
}

const textHelp = "" +
	"+Commands:\r\n" +
	"+  PING\r\n" +
	"+  GET <key>\r\n" +
	"+  SET <key> <value> [ttl_ms]\r\n" +
	"+  DEL <key>\r\n" +
	"+  EXISTS <key>\r\n" +
	"+  EXPIRE <key> <ttl_ms>\r\n" +
	"+  TTL <key>\r\n" +
	"+  KEYS [prefix]\r\n" +
	"+  STATS\r\n" +
	"+  SNAPSHOT\r\n" +
	"+  QUIT\r\n" +
	"+Note: tokens are whitespace-delimited; use kvctl for binary keys.\r\n"
