//go:build linux

package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

const (
	defaultSocket  = "/run/tc-cache/manager.sock"
	defaultPins    = "/pins/pins.json"
	defaultCache   = "/cache/v2"
	maxConnections = 8
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, os.Args[1:]); err != nil {
		log.Fatal(err)
	}
}

func run(ctx context.Context, args []string) error {
	if len(args) == 0 {
		return errors.New("expected serve, seed, stats, or hold")
	}
	flags := flag.NewFlagSet(args[0], flag.ContinueOnError)
	switch args[0] {
	case "seed-local":
		root := flags.String("root", "/objects", "output object root")
		if err := flags.Parse(args[1:]); err != nil {
			return err
		}
		return seedLocal(*root)

	case "serve":
		socket := flags.String("socket", defaultSocket, "Unix socket path")
		root := flags.String("cache", defaultCache, "cache filesystem root")
		high := flags.Int64("high", 6<<20, "high watermark in bytes")
		low := flags.Int64("low", 3<<20, "low watermark in bytes")
		if err := flags.Parse(args[1:]); err != nil {
			return err
		}
		source, err := configuredSource()
		if err != nil {
			return err
		}
		return serve(ctx, *socket, *root, source, *high, *low)
	case "seed":
		pinsPath := flags.String("pins", defaultPins, "file for generated request pins")
		if err := flags.Parse(args[1:]); err != nil {
			return err
		}
		source, err := newObjectSource()
		if err != nil {
			return err
		}
		ctx, cancel := context.WithTimeout(ctx, 60*time.Second)
		defer cancel()
		return source.seed(ctx, *pinsPath)
	case "stats":
		socket := flags.String("socket", defaultSocket, "Unix socket path")
		if err := flags.Parse(args[1:]); err != nil {
			return err
		}
		stats, err := queryStats(ctx, *socket)
		if err != nil {
			return err
		}
		return json.NewEncoder(os.Stdout).Encode(stats)
	case "hold":
		<-ctx.Done()
		return nil
	default:
		return fmt.Errorf("unknown command %q", args[0])
	}
}

func serve(ctx context.Context, socket, root string, source BundleSource, high, low int64) error {
	if err := os.MkdirAll(root, 0700); err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(socket), 0750); err != nil {
		return err
	}
	// A single process owns this cache namespace.
	lock, err := os.OpenFile(filepath.Join(root, ".owner.lock"), os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return err
	}
	defer lock.Close()
	if err := unix.Flock(int(lock.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		return fmt.Errorf("cache already has an owner: %w", err)
	}

	// setting context
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// clean existing temp directories, load cache entry
	c, err := newCache(ctx, root, source, high, low)
	if err != nil {
		return err
	}

	// verify socket
	info, err := os.Lstat(socket)
	switch {
	case err == nil && info.Mode()&os.ModeSocket != 0:
		if err := os.Remove(socket); err != nil {
			return err
		}
	case err == nil:
		return errors.New("socket path already exists and is not a socket")
	case !errors.Is(err, os.ErrNotExist):
		return err
	}

	// make socket listener
	listener, err := net.ListenUnix("unixpacket", &net.UnixAddr{Name: socket, Net: "unixpacket"})
	if err != nil {
		return err
	}
	defer listener.Close()

	// grant process to modify socket
	if err := os.Chmod(socket, 0o660); err != nil {
		return err
	}

	// close listner on context cancelation
	stop := context.AfterFunc(ctx, func() { listener.Close() })
	defer stop()

	// inform socket is ready
	log.Printf("ready on %s", socket)

	// max conn management - as `slots` is buffered channel with maxConnections capacity
	// slot remains in slots while corresponding conn is handling.
	slots := make(chan struct{}, maxConnections)
	// synchronize request with wait group
	var requests sync.WaitGroup
	defer func() {
		cancel()
		requests.Wait()
		c.loads.Wait()
	}()

	// main loop
	for {
		// accept conn
		conn, err := listener.AcceptUnix()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}

		select {
		case slots <- struct{}{}:
			requests.Add(1)
			go func() {
				defer requests.Done()

				// set context cancellation function
				stop := context.AfterFunc(ctx, func() { conn.Close() })
				defer stop()

				// return conn
				defer func() { <-slots }()
				defer conn.Close()

				// actual handling
				handle(ctx, conn, c)
			}()
		default:
			_ = conn.SetWriteDeadline(time.Now().Add(50 * time.Millisecond))
			sendFailure(conn, errCapacity)
			conn.Close()
		}
	}
}
