// Command logforge 是采集代理入口。
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/internal-platform/logforge/internal/config"
	_ "github.com/internal-platform/logforge/internal/parser" // 注册内置解析器
	"github.com/internal-platform/logforge/internal/pipeline"
	_ "github.com/internal-platform/logforge/internal/sink" // 注册内置输出端
)

func main() {
	var (
		cfgPath    = flag.String("config", "configs/logforge.yaml", "path to config file")
		metricsOut = flag.String("metrics-out", "", "write metrics JSON to this path on exit")
	)
	flag.Parse()

	cfg, err := config.Load(*cfgPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "load config: %v\n", err)
		os.Exit(1)
	}

	p, err := pipeline.New(cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "build pipeline: %v\n", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	off, err := p.Run(ctx)
	snapshot := p.Metrics()

	if *metricsOut != "" {
		raw, _ := json.MarshalIndent(snapshot, "", "  ")
		_ = os.WriteFile(*metricsOut, raw, 0o644)
	}

	fmt.Fprintf(os.Stderr, "stopped at offset %d, metrics: %v\n", off, snapshot)
	if err != nil && err != context.Canceled {
		os.Exit(1)
	}
}
