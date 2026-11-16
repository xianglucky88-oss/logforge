// Command mocklog 生成测试用日志文件：混合 JSON、logfmt、多行堆栈与坏行。
//
// 用途：给 logforge 提供确定性的输入，并支持轮转与截断两种场景。
// 同时把各类记录的数量写成 manifest，供 verify.sh 与采集端计数对账。
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"time"
)

// manifest 是写给校验脚本的记录清单。
type manifest struct {
	Records   int `json:"records"`   // 逻辑记录总数
	Lines     int `json:"lines"`     // 物理行总数（堆栈占多行，片段不产生换行）
	Stacks    int `json:"stacks"`    // 多行堆栈记录
	Bad       int `json:"bad"`       // 无法解析的坏行
	JSON      int `json:"json"`      // 完好的 JSON 记录
	Logfmt    int `json:"logfmt"`    // 完好的 logfmt 记录
	Fragments int `json:"fragments"` // 未换行片段粘连损坏的记录
}

func main() {
	var (
		path          = flag.String("path", "/tmp/logforge/app.log", "log file path")
		lines         = flag.Int("lines", 200, "number of records to write")
		intervalMS    = flag.Int("interval-ms", 5, "delay between records")
		rotateEvery   = flag.Int("rotate-every", 0, "rotate after N records (0 = never)")
		badRatio      = flag.Float64("bad-ratio", 0.05, "fraction of malformed lines")
		stackEvery    = flag.Int("stack-every", 7, "emit a multiline stack trace every N records")
		fragmentEvery = flag.Int("fragment-every", 0, "corrupt one record with an unterminated fragment every N records (0 = never)")
		manifestPath  = flag.String("manifest", "", "write record counts JSON here for verification")
		seed          = flag.Int64("seed", 20261114, "random seed")
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

	mf := manifest{Records: *lines}

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
			mf.Stacks++
			mf.Lines += 4
		case *fragmentEvery > 0 && i%*fragmentEvery == 1:
			// 未换行片段：上一次写入中断留下的半个 JSON，
			// 与本次记录的首行粘连。损坏且只损坏这一条记录，
			// 因此计入 fragments 而不是 json。
			fmt.Fprint(f, `{"ts":"2026-11-15T10:00:0`)
			fmt.Fprintf(f, `{"ts":"%s","level":"%s","service":"%s","msg":"request ok","latency_ms":%d}`+"\n",
				time.Now().UTC().Format(time.RFC3339), level, svc, rnd.Intn(400))
			mf.Fragments++
			mf.Lines++
		case rnd.Float64() < *badRatio:
			// 坏行：既不是 JSON 也不是 logfmt。
			fmt.Fprintf(f, "this line is broken %d %s\n", i, svc)
			mf.Bad++
			mf.Lines++
		case rnd.Intn(2) == 0:
			fmt.Fprintf(f, `{"ts":"%s","level":"%s","service":"%s","msg":"request ok","latency_ms":%d}`+"\n",
				time.Now().UTC().Format(time.RFC3339), level, svc, rnd.Intn(400))
			mf.JSON++
			mf.Lines++
		default:
			fmt.Fprintf(f, "ts=%s level=%s service=%s msg=%q code=%d\n",
				time.Now().UTC().Format(time.RFC3339), level, svc, "handled", 200+rnd.Intn(5)*100)
			mf.Logfmt++
			mf.Lines++
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

	if *manifestPath != "" {
		raw, err := json.MarshalIndent(mf, "", "  ")
		if err != nil {
			fmt.Fprintf(os.Stderr, "marshal manifest: %v\n", err)
			os.Exit(1)
		}
		if err := os.WriteFile(*manifestPath, raw, 0o644); err != nil {
			fmt.Fprintf(os.Stderr, "write manifest: %v\n", err)
			os.Exit(1)
		}
	}
	fmt.Fprintf(os.Stderr, "wrote %d records to %s (%+v)\n", *lines, *path, mf)
}
