// Command mocklog 生成测试用日志文件：混合 JSON、logfmt、多行堆栈与坏行。
//
// 用途：给 logforge 提供确定性的输入，并支持轮转与截断两种场景。
package main

import (
	"flag"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"time"
)

func main() {
	var (
		path        = flag.String("path", "/tmp/logforge/app.log", "log file path")
		lines       = flag.Int("lines", 200, "number of records to write")
		intervalMS  = flag.Int("interval-ms", 5, "delay between records")
		rotateEvery = flag.Int("rotate-every", 0, "rotate after N records (0 = never)")
		badRatio    = flag.Float64("bad-ratio", 0.05, "fraction of malformed lines")
		stackEvery  = flag.Int("stack-every", 7, "emit a multiline stack trace every N records")
		seed        = flag.Int64("seed", 20261114, "random seed")
	)
	flag.Parse()

	if err := os.MkdirAll(filepath.Dir(*path), 0o755); err != nil {
		fmt.Fprintf(os.Stderr, "mkdir: %v\n", err)
		os.Exit(1)
	}

	rnd := rand.New(rand.NewSource(*seed))
	f, err := os.OpenFile(*path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		fmt.Fprintf(os.Stderr, "open: %v\n", err)
		os.Exit(1)
	}
	defer f.Close()

	levels := []string{"INFO", "WARN", "ERROR", "DEBUG"}
	services := []string{"gateway", "orders", "billing", "search"}

	for i := 0; i < *lines; i++ {
		level := levels[rnd.Intn(len(levels))]
		svc := services[rnd.Intn(len(services))]

		switch {
		case *stackEvery > 0 && i%*stackEvery == 0:
			// 多行记录：首行带时间戳，后面是缩进的堆栈。
			fmt.Fprintf(f, "%s %s %s panic recovered\n",
				time.Now().UTC().Format("2006-01-02T15:04:05Z"), level, svc)
			for d := 1; d <= 3; d++ {
				fmt.Fprintf(f, "    at frame%d(%s.go:%d)\n", d, svc, d*11)
			}
		case rnd.Float64() < *badRatio:
			// 坏行：既不是 JSON 也不是 logfmt。
			fmt.Fprintf(f, "this line is broken %d %s\n", i, svc)
		case rnd.Intn(2) == 0:
			fmt.Fprintf(f, `{"ts":"%s","level":"%s","service":"%s","msg":"request ok","latency_ms":%d}`+"\n",
				time.Now().UTC().Format(time.RFC3339), level, svc, rnd.Intn(400))
		default:
			fmt.Fprintf(f, "ts=%s level=%s service=%s msg=%q code=%d\n",
				time.Now().UTC().Format(time.RFC3339), level, svc, "handled", 200+rnd.Intn(5)*100)
		}

		if *intervalMS > 0 {
			time.Sleep(time.Duration(*intervalMS) * time.Millisecond)
		}

		if *rotateEvery > 0 && (i+1)%*rotateEvery == 0 {
			f.Close()
			_ = os.Rename(*path, fmt.Sprintf("%s.%d", *path, (i+1) / *rotateEvery))
			f, err = os.OpenFile(*path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
			if err != nil {
				fmt.Fprintf(os.Stderr, "reopen: %v\n", err)
				os.Exit(1)
			}
		}
	}
	fmt.Fprintf(os.Stderr, "wrote %d records to %s\n", *lines, *path)
}
