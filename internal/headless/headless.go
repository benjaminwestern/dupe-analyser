// internal/headless/headless.go
package headless

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/benjaminwestern/dupe-analyser/internal/analyser"
	"github.com/benjaminwestern/dupe-analyser/internal/report"
	"github.com/benjaminwestern/dupe-analyser/internal/source"
)

// Config holds the settings required for a headless run.
type Config struct {
	Paths               string
	Key                 string
	Workers             int
	LogPath             string
	OutputFormat        string
	ValidateOnly        bool
	CheckKey            bool
	CheckRow            bool
	ShowFolderBreakdown bool
	EnableTxtOutput     bool
	EnableJsonOutput    bool
}

// Run executes the full analysis in headless (non-interactive) mode.
func Run(ctx context.Context, cfg *Config) {
	if cfg.ValidateOnly {
		fmt.Println("Running in Key Validation Mode...")
	} else {
		fmt.Println("Running in headless mode...")
	}
	startTime := time.Now()

	pathStrings := strings.Split(cfg.Paths, ",")
	for i, p := range pathStrings {
		pathStrings[i] = strings.TrimSpace(p)
	}

	sources, err := source.DiscoverAll(ctx, pathStrings)
	if err != nil {
		fmt.Printf("Error discovering sources: %v\n", err)
		return
	}
	fmt.Printf("Discovered %d files to analyse across %d path(s).\n", len(sources), len(pathStrings))

	eng := analyser.New(cfg.Key, cfg.Workers, cfg.CheckKey, cfg.CheckRow, cfg.ValidateOnly)
	finalReport := eng.Run(ctx, sources)

	finalReport.Summary.TotalElapsedTime = time.Since(startTime).Round(time.Second).String()
	filenameBase := report.SaveAndLog(finalReport, cfg.LogPath, cfg.EnableTxtOutput, cfg.EnableJsonOutput, cfg.CheckKey, cfg.CheckRow, cfg.ShowFolderBreakdown)

	if !cfg.ValidateOnly && (cfg.EnableTxtOutput || cfg.EnableJsonOutput) {
		var parts []string
		if cfg.EnableTxtOutput {
			parts = append(parts, ".txt")
		}
		if cfg.EnableJsonOutput {
			parts = append(parts, ".json")
		}
		fmt.Printf("Analysis complete. Reports saved with base name '%s' and extension(s): %s\n", filenameBase, strings.Join(parts, ", "))
	} else if !cfg.ValidateOnly {
		fmt.Println("Analysis complete. No report files were generated as per configuration.")
	}

	if cfg.OutputFormat == "json" {
		jsonReport, _ := finalReport.ToJSON()
		fmt.Println(jsonReport)
	} else {
		fmt.Println("\n" + finalReport.String(true, cfg.CheckKey, cfg.CheckRow, cfg.ShowFolderBreakdown))
	}
}
