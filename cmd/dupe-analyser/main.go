// cmd/dupe-analyser/main.go
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/benjaminwestern/dupe-analyser/internal/config"
	"github.com/benjaminwestern/dupe-analyser/internal/headless"
	"github.com/benjaminwestern/dupe-analyser/internal/tui"
)

// main holds the logic for the application's main entry point.
func main() {
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("Error loading configuration: %v", err)
	}

	var isHeadless bool
	var isValidate bool
	var outputFormat string
	var keyIsSet bool

	flag.StringVar(&cfg.Path, "path", cfg.Path, "Comma-separated list of paths to analyse (local or GCS)")
	flag.StringVar(&cfg.Key, "key", cfg.Key, "JSON key for uniqueness check")
	flag.IntVar(&cfg.Workers, "workers", cfg.Workers, "Number of concurrent workers")
	flag.StringVar(&cfg.LogPath, "log-path", cfg.LogPath, "Directory to save logs and reports")
	flag.BoolVar(&cfg.CheckKey, "check.key", cfg.CheckKey, "Enable duplicate key check")
	flag.BoolVar(&cfg.CheckRow, "check.row", cfg.CheckRow, "Enable duplicate row check (hashing)")
	flag.BoolVar(&cfg.ShowFolderBreakdown, "show.folders", cfg.ShowFolderBreakdown, "Show per-folder breakdown table in summary report")
	flag.BoolVar(&cfg.EnableTxtOutput, "output.txt", cfg.EnableTxtOutput, "Enable .txt report output")
	flag.BoolVar(&cfg.EnableJsonOutput, "output.json", cfg.EnableJsonOutput, "Enable .json report output")
	flag.BoolVar(&cfg.PurgeIDs, "purge-ids", cfg.PurgeIDs, "Enable interactive purging of duplicate IDs (local files only)")
	flag.BoolVar(&cfg.PurgeRows, "purge-rows", cfg.PurgeRows, "Enable interactive purging of duplicate rows (local files only)")
	flag.BoolVar(&isHeadless, "headless", false, "Run without TUI and print report to stdout")
	flag.BoolVar(&isValidate, "validate", false, "Run a key validation test and exit (headless only)")
	flag.StringVar(&outputFormat, "output", "txt", "Output format for headless mode (txt or json)")
	flag.Parse()

	flag.Visit(func(f *flag.Flag) {
		if f.Name == "key" {
			keyIsSet = true
		}
	})

	isGCSPath := strings.Contains(cfg.Path, "gs://")
	if isGCSPath && (cfg.PurgeIDs || cfg.PurgeRows) {
		fmt.Println("Error: Purge functionality is only available for local files, not for GCS paths.")
		os.Exit(1)
	}
	if !isHeadless && cfg.Path == "" && flag.NArg() > 0 {
		cfg.Path = strings.Join(flag.Args(), ",")
	}

	if err := os.MkdirAll(cfg.LogPath, 0755); err != nil {
		log.Fatalf("failed to create log directory at %s: %v", cfg.LogPath, err)
	}
	logFilePath := filepath.Join(cfg.LogPath, "analyser.log")
	logFile, err := os.OpenFile(logFilePath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0666)
	if err != nil {
		log.Fatalf("failed to open log file at %s: %v", logFilePath, err)
	}
	defer logFile.Close()
	log.SetOutput(logFile)

	if isHeadless || isValidate {
		if cfg.Path == "" {
			fmt.Println("Error: -path flag is required for headless/validation mode.")
			os.Exit(1)
		}
		if cfg.Key == "" {
			fmt.Println("Error: -key flag is required for validation mode.")
			os.Exit(1)
		}
		if isHeadless && !isValidate && !cfg.CheckKey && !cfg.CheckRow {
			fmt.Println("Error: At least one check (-check.key or -check.row) must be enabled for a full analysis.")
			os.Exit(1)
		}
		if cfg.CheckKey && !keyIsSet {
			fmt.Println("Warning: -key flag not set, defaulting to 'id'.")
		}

		headlessCfg := &headless.Config{
			Paths:               cfg.Path,
			Key:                 cfg.Key,
			Workers:             cfg.Workers,
			LogPath:             cfg.LogPath,
			OutputFormat:        outputFormat,
			ValidateOnly:        isValidate,
			CheckKey:            cfg.CheckKey,
			CheckRow:            cfg.CheckRow,
			ShowFolderBreakdown: cfg.ShowFolderBreakdown,
			EnableTxtOutput:     cfg.EnableTxtOutput,
			EnableJsonOutput:    cfg.EnableJsonOutput,
		}

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		headless.Run(ctx, headlessCfg)
		return
	}

	if !cfg.CheckKey && !cfg.CheckRow {
		fmt.Println("Error: At least one check (-check.key or -check.row) must be enabled.")
		os.Exit(1)
	}

	currentConfig := cfg
	for {
		finalConfig, shouldRestart, startNew, err := tui.Run(currentConfig)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error running application: %v\n", err)
			os.Exit(1)
		}
		if !shouldRestart {
			break
		}

		if startNew {
			newCfg, loadErr := config.Load()
			if loadErr != nil {
				log.Fatalf("Error reloading configuration for new job: %v", loadErr)
			}
			newCfg.LogPath = cfg.LogPath
			currentConfig = newCfg
		} else {
			currentConfig = finalConfig
		}
	}
}
