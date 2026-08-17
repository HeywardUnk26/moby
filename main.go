package main

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// Message represents a log message.
type Message struct {
	Line string
	Time time.Time
}

// Container represents a mock container.
type Container struct {
	mu       sync.Mutex
	id       string
	running  bool
	logs     []Message
	logCh    chan Message
	exitCh   chan struct{}
}

func NewContainer(id string) *Container {
	return &Container{
		id:      id,
		running: true,
		logCh:   make(chan Message, 100),
		exitCh:  make(chan struct{}),
	}
}

func (c *Container) WriteLog(line string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	msg := Message{Line: line, Time: time.Now()}
	c.logs = append(c.logs, msg)
	if c.running {
		select {
		case c.logCh <- msg:
		default:
		}
	}
}

func (c *Container) Stop() {
	c.mu.Lock()
	if !c.running {
		c.mu.Unlock()
		return
	}
	c.running = false
	close(c.exitCh)
	c.mu.Unlock()
}

func (c *Container) IsRunning() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.running
}

func (c *Container) Wait() <-chan struct{} {
	return c.exitCh
}

// Logger defines the interface for reading logs.
type Logger interface {
	ReadLogs(ctx context.Context, follow bool) (<-chan Message, <-chan error)
}

// JSONFileLogger implements Logger for json-file driver.
type JSONFileLogger struct {
	container *Container
	activeGoroutines *int32
}

func (l *JSONFileLogger) ReadLogs(ctx context.Context, follow bool) (<-chan Message, <-chan error) {
	msgCh := make(chan Message)
	errCh := make(chan error, 1)

	atomic.AddInt32(l.activeGoroutines, 1)
	go func() {
		defer atomic.AddInt32(l.activeGoroutines, -1)
		defer close(msgCh)
		defer close(errCh)

		// 1. Read existing logs
		l.container.mu.Lock()
		existing := make([]Message, len(l.container.logs))
		copy(existing, l.container.logs)
		running := l.container.running
		l.container.mu.Unlock()

		for _, msg := range existing {
			select {
			case msgCh <- msg:
			case <-ctx.Done():
				errCh <- ctx.Err()
				return
			}
		}

		if !follow || !running {
			return
		}

		// 2. Follow logs
		for {
			select {
			case msg, ok := <-l.container.logCh:
				if !ok {
					return
				}
				select {
				case msgCh <- msg:
				case <-ctx.Done():
					errCh <- ctx.Err()
					return
				}
			case <-ctx.Done():
				errCh <- ctx.Err()
				return
			}
		}
	}()

	return msgCh, errCh
}

// LocalLogger implements Logger for local driver.
type LocalLogger struct {
	container *Container
	activeGoroutines *int32
}

func (l *LocalLogger) ReadLogs(ctx context.Context, follow bool) (<-chan Message, <-chan error) {
	msgCh := make(chan Message)
	errCh := make(chan error, 1)

	atomic.AddInt32(l.activeGoroutines, 1)
	go func() {
		defer atomic.AddInt32(l.activeGoroutines, -1)
		defer close(msgCh)
		defer close(errCh)

		// 1. Read existing logs
		l.container.mu.Lock()
		existing := make([]Message, len(l.container.logs))
		copy(existing, l.container.logs)
		running := l.container.running
		l.container.mu.Unlock()

		for _, msg := range existing {
			select {
			case msgCh <- msg:
			case <-ctx.Done():
				errCh <- ctx.Err()
				return
			}
		}

		if !follow || !running {
			return
		}

		// 2. Follow logs
		for {
			select {
			case msg, ok := <-l.container.logCh:
				if !ok {
					return
				}
				select {
				case msgCh <- msg:
				case <-ctx.Done():
					errCh <- ctx.Err()
					return
				}
			case <-ctx.Done():
				errCh <- ctx.Err()
				return
			}
		}
	}()

	return msgCh, errCh
}

// ContainerLogs orchestrates the log stream.
// It monitors the container state and ensures the context is cancelled when the container exits.
func ContainerLogs(ctx context.Context, c *Container, follow bool, driver Logger, activeGoroutines *int32) (<-chan Message, <-chan error) {
	outCh := make(chan Message)
	errCh := make(chan error, 1)

	// Create a cancellable context for the log reader
	logCtx, cancel := context.WithCancel(ctx)

	atomic.AddInt32(activeGoroutines, 1)
	go func() {
		defer atomic.AddInt32(activeGoroutines, -1)
		defer cancel()

		// Monitor container exit to cancel the log reader context immediately
		select {
		case <-c.Wait():
			cancel()
		case <-ctx.Done():
		}
	}()

	atomic.AddInt32(activeGoroutines, 1)
	go func() {
		defer atomic.AddInt32(activeGoroutines, -1)
		defer close(outCh)
		defer close(errCh)

		driverMsgCh, driverErrCh := driver.ReadLogs(logCtx, follow)

		for {
			select {
			case msg, ok := <-driverMsgCh:
				if !ok {
					return
				}
				select {
				case outCh <- msg:
				case <-ctx.Done():
					errCh <- ctx.Err()
					return
				}
			case err := <-driverErrCh:
				if err != nil && !errors.Is(err, context.Canceled) {
					errCh <- err
				}
				return
			case <-ctx.Done():
				errCh <- ctx.Err()
				return
			}
		}
	}()

	return outCh, errCh
}

func main() {
	var activeGoroutines int32

	fmt.Println("Starting Container Log Stream Verification...")

	// Test Case 1: Active log stream terminates immediately when container stops
	{
		fmt.Println("\n--- Test Case 1: Active log stream terminates on container stop ---")
		c := NewContainer("test-container-1")
		c.WriteLog("hello")
		c.WriteLog("world")

		driver := &JSONFileLogger{container: c, activeGoroutines: &activeGoroutines}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		outCh, errCh := ContainerLogs(ctx, c, true, driver, &activeGoroutines)

		// Read initial logs
		msg1 := <-outCh
		msg2 := <-outCh
		fmt.Printf("Received: %s\n", msg1.Line)
		fmt.Printf("Received: %s\n", msg2.Line)

		// Stop container asynchronously
		go func() {
			time.Sleep(100 * time.Millisecond)
			c.Stop()
		}()

		// Wait for stream to terminate
		select {
		case err := <-errCh:
			if err != nil {
				fmt.Printf("Stream terminated with error: %v\n", err)
			} else {
				fmt.Println("Stream terminated successfully and immediately upon container stop.")
			}
		case <-time.After(2 * time.Second):
			panic("FAIL: Stream hung after container stopped")
		}

		// Wait a bit to ensure goroutines are cleaned up
		time.Sleep(100 * time.Millisecond)
		if atomic.LoadInt32(&activeGoroutines) != 0 {
			panic(fmt.Sprintf("FAIL: Leaked %d goroutines", atomic.LoadInt32(&activeGoroutines)))
		}
		fmt.Println("No goroutines leaked.")
	}

	// Test Case 2: Already stopped container with follow=true terminates immediately
	{
		fmt.Println("\n--- Test Case 2: Already stopped container terminates immediately ---")
		c := NewContainer("test-container-2")
		c.WriteLog("existing log")
		c.Stop()

		driver := &LocalLogger{container: c, activeGoroutines: &activeGoroutines}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		outCh, errCh := ContainerLogs(ctx, c, true, driver, &activeGoroutines)

		// Read existing logs
		msg := <-outCh
		fmt.Printf("Received: %s\n", msg.Line)

		// Stream should terminate immediately
		select {
		case err := <-errCh:
			if err != nil {
				fmt.Printf("Stream terminated with error: %v\n", err)
			} else {
				fmt.Println("Stream terminated successfully and immediately for already stopped container.")
			}
		case <-time.After(2 * time.Second):
			panic("FAIL: Stream hung for already stopped container")
		}

		// Wait a bit to ensure goroutines are cleaned up
		time.Sleep(100 * time.Millisecond)
		if atomic.LoadInt32(&activeGoroutines) != 0 {
			panic(fmt.Sprintf("FAIL: Leaked %d goroutines", atomic.LoadInt32(&activeGoroutines)))
		}
		fmt.Println("No goroutines leaked.")
	}

	fmt.Println("\nAll verification tests passed successfully!")
}
