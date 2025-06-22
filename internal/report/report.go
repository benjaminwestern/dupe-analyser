// internal/report/report.go
package report

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/charmbracelet/lipgloss"
)

// LocationInfo holds the location of a piece of data.
type LocationInfo struct {
	FilePath   string `json:"filePath"`
	LineNumber int    `json:"lineNumber"`
}

// JSONData is a generic type for a single JSON object.
type JSONData map[string]interface{}

// FolderDetail holds aggregated metrics for a single folder/prefix.
type FolderDetail struct {
	ProcessedSizeBytes int64 `json:"processedSizeBytes"`
	TotalSizeBytes     int64 `json:"totalSizeBytes"`
	FilesProcessed     int   `json:"filesProcessed"`
	TotalFiles         int   `json:"totalFiles"`
	KeysFound          int   `json:"keysFound"`
	RowsProcessed      int   `json:"rowsProcessed"`
}

// AnalysisReport is the top-level structure for the entire analysis result.
type AnalysisReport struct {
	Summary       SummaryReport             `json:"summary"`
	DuplicateIDs  map[string][]LocationInfo `json:"duplicateIds"`
	DuplicateRows map[string][]LocationInfo `json:"duplicateRows"`
}

// SummaryReport contains aggregated metrics from the analysis.
type SummaryReport struct {
	IsValidationReport        bool                      `json:"isValidationReport"`
	IsPartialReport           bool                      `json:"isPartialReport"`
	FilesProcessed            int32                     `json:"filesProcessed"`
	TotalFiles                int                       `json:"totalFiles"`
	ProcessedDataSizeBytes    int64                     `json:"processedDataSizeBytes"`
	TotalDataSizeOverallBytes int64                     `json:"totalDataSizeOverallBytes"`
	ProcessedDataSizeHuman    string                    `json:"processedDataSizeHuman"`
	TotalDataSizeOverallHuman string                    `json:"totalDataSizeOverallHuman"`
	TotalElapsedTime          string                    `json:"totalElapsedTime"`
	TotalRowsProcessed        int64                     `json:"totalRowsProcessed"`
	UniqueKey                 string                    `json:"uniqueKey"`
	TotalKeyOccurrences       int                       `json:"totalKeyOccurrences"`
	UniqueKeysDuplicated      int                       `json:"uniqueKeysDuplicated"`
	DuplicateRowInstances     int                       `json:"duplicateRowInstances"`
	AverageRowsPerFile        float64                   `json:"averageRowsPerFile"`
	AverageFilesPerFolder     float64                   `json:"averageFilesPerFolder"`
	DuplicateIDsPerFolder     map[string]int            `json:"duplicateIDsPerFolder"`
	DuplicateRowsPerFolder    map[string]int            `json:"duplicateRowsPerFolder"`
	FolderDetails             map[string]FolderDetail `json:"folderDetails"`
}

var (
	reportStyle      = lipgloss.NewStyle().Padding(0, 1).Border(lipgloss.RoundedBorder()).BorderForeground(lipgloss.Color("63"))
	headerStyle      = lipgloss.NewStyle().Bold(true).MarginBottom(1).Underline(true)
	tableHeaderStyle = lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("212"))
)

// HumanSize returns a human-readable string for a given byte size.
func HumanSize(bytes int64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}
	div, exp := int64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(bytes)/float64(div), "KMGTPE"[exp])
}

// String formats the report for display.
func (r *AnalysisReport) String(isFullReport bool, checkKey, checkRow, showFolderBreakdown bool) string {
	if r.Summary.IsValidationReport {
		return r.validationReportString(showFolderBreakdown)
	}
	return r.analysisReportString(isFullReport, checkKey, checkRow, showFolderBreakdown)
}

func (r *AnalysisReport) validationReportString(showFolderBreakdown bool) string {
	s := r.Summary
	var b strings.Builder

	b.WriteString(headerStyle.Render("--- Key Validation Summary ---") + "\n")
	filesAnalysedStr := fmt.Sprintf("%d", s.FilesProcessed)
	if s.IsPartialReport {
		filesAnalysedStr = fmt.Sprintf("%d of %d", s.FilesProcessed, s.TotalFiles)
	}

	summaryContent := fmt.Sprintf(
		"Key to Find:                  '%s'\nTotal Files Analysed:           %s\nTotal Rows Processed:           %d\nTotal Keys Found:             %d\nTotal Elapsed Time:           %s",
		s.UniqueKey, filesAnalysedStr, s.TotalRowsProcessed, s.TotalKeyOccurrences, s.TotalElapsedTime,
	)
	b.WriteString(reportStyle.Render(summaryContent))

	if showFolderBreakdown && len(s.FolderDetails) > 0 {
		var sortedFolders []string
		for path := range s.FolderDetails {
			sortedFolders = append(sortedFolders, path)
		}
		sort.Strings(sortedFolders)

		var tableContent strings.Builder
		headers := []string{"Path", "Files Checked", "Rows Processed", "Keys Found"}

		type formattedRow struct{ path, files, rows, keys string }
		var rows []formattedRow
		maxWidths := make([]int, len(headers))
		for i, h := range headers {
			maxWidths[i] = len(h)
		}

		for _, folder := range sortedFolders {
			detail := s.FolderDetails[folder]
			var filesStr string
			if s.IsPartialReport {
				filesStr = fmt.Sprintf("%d / %d", detail.FilesProcessed, detail.TotalFiles)
			} else {
				filesStr = fmt.Sprintf("%d", detail.TotalFiles)
			}
			row := formattedRow{
				path:  folder,
				files: filesStr,
				rows:  fmt.Sprintf("%d", detail.RowsProcessed),
				keys:  fmt.Sprintf("%d", detail.KeysFound),
			}
			rows = append(rows, row)

			if len(row.path) > maxWidths[0] { maxWidths[0] = len(row.path) }
			if len(row.files) > maxWidths[1] { maxWidths[1] = len(row.files) }
			if len(row.rows) > maxWidths[2] { maxWidths[2] = len(row.rows) }
			if len(row.keys) > maxWidths[3] { maxWidths[3] = len(row.keys) }
		}

		headerFormat := fmt.Sprintf("%%-%ds | %%-%ds | %%-%ds | %%-%ds", maxWidths[0], maxWidths[1], maxWidths[2], maxWidths[3])
		headerLine := fmt.Sprintf(headerFormat, headers[0], headers[1], headers[2], headers[3])
		tableContent.WriteString(tableHeaderStyle.Render(headerLine) + "\n")

		rowFormat := fmt.Sprintf("%%-%ds | %%-%ds | %%-%ds | %%-%ds", maxWidths[0], maxWidths[1], maxWidths[2], maxWidths[3])
		for _, row := range rows {
			tableContent.WriteString(fmt.Sprintf(rowFormat, row.path, row.files, row.rows, row.keys) + "\n")
		}

		b.WriteString("\n\n" + headerStyle.Render("--- Per-Folder Breakdown ---") + "\n")
		b.WriteString(reportStyle.Render(strings.TrimRight(tableContent.String(), "\n")))
	}

	return b.String()
}

func (r *AnalysisReport) analysisReportString(isFullReport bool, checkKey, checkRow, showFolderBreakdown bool) string {
	s := r.Summary
	var b strings.Builder

	b.WriteString(headerStyle.Render("--- Analysis Summary ---") + "\n")
	filesAnalysedStr := fmt.Sprintf("%d", s.FilesProcessed)
	dataAnalysedStr := s.ProcessedDataSizeHuman
	if s.IsPartialReport {
		filesAnalysedStr = fmt.Sprintf("%d of %d", s.FilesProcessed, s.TotalFiles)
		dataAnalysedStr = fmt.Sprintf("%s of %s", s.ProcessedDataSizeHuman, s.TotalDataSizeOverallHuman)
	}

	summaryContent := fmt.Sprintf(
		"Total Elapsed Time:           %s\nTotal Files Analysed:         %s\nTotal Data Analysed:          %s\nAverage Rows Per File (Global): %.2f\nAverage Files Per Folder:     %.2f",
		s.TotalElapsedTime, filesAnalysedStr, dataAnalysedStr, s.AverageRowsPerFile, s.AverageFilesPerFolder,
	)
	if checkKey {
		summaryContent += fmt.Sprintf("\nTotal Occurrences of '%s':  %d\nUnique '%s's with Duplicates: %d", s.UniqueKey, s.TotalKeyOccurrences, s.UniqueKey, s.UniqueKeysDuplicated)
	}
	if checkRow {
		summaryContent += fmt.Sprintf("\nTotal Duplicate Row Instances:  %d", s.DuplicateRowInstances)
	}
	b.WriteString(reportStyle.Render(summaryContent))

	if showFolderBreakdown && len(s.FolderDetails) > 0 {
		var sortedFolders []string
		for path := range s.FolderDetails {
			sortedFolders = append(sortedFolders, path)
		}
		sort.Strings(sortedFolders)

		var tableContent strings.Builder
		headers := []string{"Path", "Data Analysed", "Files Analysed", "Avg Rows/File", "Rows Processed", "Keys Found", "Duplicate IDs", "Duplicate Rows"}

		type formattedRow struct {
			path, data, files, avgRows, rows, keys, dupeIDs, dupeRows string
		}
		var rows []formattedRow
		maxWidths := make([]int, len(headers))
		for i, h := range headers {
			maxWidths[i] = len(h)
		}

		for _, folder := range sortedFolders {
			detail := s.FolderDetails[folder]
			
			var dataStr, filesStr string
			if s.IsPartialReport {
				dataStr = fmt.Sprintf("%s / %s", HumanSize(detail.ProcessedSizeBytes), HumanSize(detail.TotalSizeBytes))
				filesStr = fmt.Sprintf("%d / %d", detail.FilesProcessed, detail.TotalFiles)
			} else {
				dataStr = HumanSize(detail.TotalSizeBytes)
				filesStr = fmt.Sprintf("%d", detail.TotalFiles)
			}

			var avgRowsPerFile float64
			if detail.FilesProcessed > 0 {
				avgRowsPerFile = float64(detail.RowsProcessed) / float64(detail.FilesProcessed)
			}

			idCount := s.DuplicateIDsPerFolder[folder]
			rowCount := s.DuplicateRowsPerFolder[folder]

			row := formattedRow{
				path:     folder,
				data:     dataStr,
				files:    filesStr,
				avgRows:  fmt.Sprintf("%.2f", avgRowsPerFile),
				rows:     fmt.Sprintf("%d", detail.RowsProcessed),
				keys:     fmt.Sprintf("%d", detail.KeysFound),
				dupeIDs:  fmt.Sprintf("%d", idCount),
				dupeRows: fmt.Sprintf("%d", rowCount),
			}
			rows = append(rows, row)
			
			if len(row.path) > maxWidths[0] { maxWidths[0] = len(row.path) }
			if len(row.data) > maxWidths[1] { maxWidths[1] = len(row.data) }
			if len(row.files) > maxWidths[2] { maxWidths[2] = len(row.files) }
			if len(row.avgRows) > maxWidths[3] { maxWidths[3] = len(row.avgRows) }
			if len(row.rows) > maxWidths[4] { maxWidths[4] = len(row.rows) }
			if len(row.keys) > maxWidths[5] { maxWidths[5] = len(row.keys) }
			if len(row.dupeIDs) > maxWidths[6] { maxWidths[6] = len(row.dupeIDs) }
			if len(row.dupeRows) > maxWidths[7] { maxWidths[7] = len(row.dupeRows) }
		}
		
		headerFormat := fmt.Sprintf("%%-%ds | %%-%ds | %%-%ds | %%-%ds | %%-%ds | %%-%ds | %%-%ds | %%-%ds", maxWidths[0], maxWidths[1], maxWidths[2], maxWidths[3], maxWidths[4], maxWidths[5], maxWidths[6], maxWidths[7])
		headerLine := fmt.Sprintf(headerFormat, headers[0], headers[1], headers[2], headers[3], headers[4], headers[5], headers[6], headers[7])
		tableContent.WriteString(tableHeaderStyle.Render(headerLine) + "\n")

		rowFormat := fmt.Sprintf("%%-%ds | %%-%ds | %%-%ds | %%-%ds | %%-%ds | %%-%ds | %%-%ds | %%-%ds", maxWidths[0], maxWidths[1], maxWidths[2], maxWidths[3], maxWidths[4], maxWidths[5], maxWidths[6], maxWidths[7])
		for _, row := range rows {
			tableContent.WriteString(fmt.Sprintf(rowFormat, row.path, row.data, row.files, row.avgRows, row.rows, row.keys, row.dupeIDs, row.dupeRows) + "\n")
		}

		b.WriteString("\n\n" + headerStyle.Render("--- Per-Folder Breakdown ---") + "\n")
		b.WriteString(reportStyle.Render(strings.TrimRight(tableContent.String(), "\n")))
	}

	if isFullReport {
		if checkKey && len(r.DuplicateIDs) > 0 {
			b.WriteString("\n\n" + headerStyle.Render("--- Full Duplicate ID Details ---"))
			ids := make([]string, 0, len(r.DuplicateIDs))
			for id := range r.DuplicateIDs {
				ids = append(ids, id)
			}
			sort.Strings(ids)
			for _, id := range ids {
				locs := r.DuplicateIDs[id]
				b.WriteString(fmt.Sprintf("\nID '%s': %s (appears %d times)\n", s.UniqueKey, id, len(locs)))
				for _, loc := range locs {
					b.WriteString(fmt.Sprintf("  - File: %s, Row: %d\n", loc.FilePath, loc.LineNumber))
				}
			}
		}
		if checkRow && len(r.DuplicateRows) > 0 {
			b.WriteString("\n\n" + headerStyle.Render("--- Full Duplicate Row Details ---"))
			hashes := make([]string, 0, len(r.DuplicateRows))
			for hash := range r.DuplicateRows {
				hashes = append(hashes, hash)
			}
			sort.Strings(hashes)
			for _, hash := range hashes {
				locs := r.DuplicateRows[hash]
				b.WriteString(fmt.Sprintf("\nRow (Hash: %s) found %d times:\n", hash, len(locs)))
				for _, loc := range locs {
					b.WriteString(fmt.Sprintf("  - File: %s, Row: %d\n", loc.FilePath, loc.LineNumber))
				}
			}
		}
	}
	return b.String()
}


// ToJSON converts the report to a JSON string.
func (r *AnalysisReport) ToJSON() (string, error) {
	bytes, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		return "", fmt.Errorf("could not marshal report to json: %w", err)
	}
	return string(bytes), err
}

// Save saves the report to disk based on configuration.
func (r *AnalysisReport) Save(baseFilename string, enableTxt, enableJson, checkKey, checkRow, showFolderBreakdown bool) {
	if enableTxt {
		summaryFilename := baseFilename + "_summary.txt"
		detailsFilename := baseFilename + "_details.txt"
		if err := os.WriteFile(summaryFilename, []byte(r.String(false, checkKey, checkRow, showFolderBreakdown)), 0644); err != nil {
			log.Printf("Failed to save TXT summary report to %s: %v", summaryFilename, err)
		}
		if err := os.WriteFile(detailsFilename, []byte(r.String(true, checkKey, checkRow, showFolderBreakdown)), 0644); err != nil {
			log.Printf("Failed to save TXT details report to %s: %v", detailsFilename, err)
		}
	}
	if enableJson {
		filename := baseFilename + ".json"
		jsonData, err := r.ToJSON()
		if err != nil {
			log.Printf("Failed to marshal JSON report: %v", err)
			return
		}
		if err := os.WriteFile(filename, []byte(jsonData), 0644); err != nil {
			log.Printf("Failed to save JSON report to %s: %v", filename, err)
		}
	}
}

// SaveAndLog generates a timestamped filename inside the given logPath, saves the
// report, and returns the base filename.
func SaveAndLog(rep *AnalysisReport, logPath string, enableTxt, enableJson, checkKey, checkRow, showFolderBreakdown bool) string {
	baseName := "report-" + time.Now().Format("2006-01-02_15-04-05")
	fullPathBase := filepath.Join(logPath, baseName)
	rep.Save(fullPathBase, enableTxt, enableJson, checkKey, checkRow, showFolderBreakdown)
	return fullPathBase
}
