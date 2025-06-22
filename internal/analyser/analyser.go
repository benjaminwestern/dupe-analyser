// internal/analyser/analyser.go
package analyser

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"hash"
	"hash/fnv"
	"log"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/benjaminwestern/dupe-analyser/internal/report"
	"github.com/benjaminwestern/dupe-analyser/internal/source"
)

// Analyser holds the state and configuration for an analysis run.
type Analyser struct {
	uniqueKey              string
	numWorkers             int
	checkKey               bool
	checkRow               bool
	ValidateOnly           bool
	idLocations            map[string][]report.LocationInfo
	idMutex                sync.Mutex
	rowHashes              map[string][]report.LocationInfo
	rowMutex               sync.Mutex
	keysFoundPerFolder     map[string]int64
	keysFoundMutex         sync.Mutex
	rowsProcessedPerFolder map[string]int64
	rowsProcessedMutex     sync.Mutex
	ProcessedFiles         *atomic.Int32
	TotalRows              *atomic.Int64
	CurrentFolder          *atomic.Value
	processedPaths         map[string]bool
	processedPathsMutex    sync.Mutex
}

// New creates a new, configured Analyser instance.
func New(uniqueKey string, numWorkers int, checkKey, checkRow, validateOnly bool) *Analyser {
	return &Analyser{
		uniqueKey:              uniqueKey,
		numWorkers:             numWorkers,
		checkKey:               checkKey,
		checkRow:               checkRow,
		ValidateOnly:           validateOnly,
		idLocations:            make(map[string][]report.LocationInfo),
		rowHashes:              make(map[string][]report.LocationInfo),
		keysFoundPerFolder:     make(map[string]int64),
		rowsProcessedPerFolder: make(map[string]int64),
		ProcessedFiles:         new(atomic.Int32),
		TotalRows:              new(atomic.Int64),
		CurrentFolder:          new(atomic.Value),
		processedPaths:         make(map[string]bool),
	}
}

// GetUnprocessedSources filters a list of all sources against the ones that have
// already been successfully processed by this analyser instance.
func (a *Analyser) GetUnprocessedSources(allSources []source.InputSource) []source.InputSource {
	a.processedPathsMutex.Lock()
	defer a.processedPathsMutex.Unlock()

	var unprocessed []source.InputSource
	for _, s := range allSources {
		if !a.processedPaths[s.Path()] {
			unprocessed = append(unprocessed, s)
		}
	}
	return unprocessed
}

// Run executes the analysis process on a given set of sources and returns a full report.
func (a *Analyser) Run(ctx context.Context, sources []source.InputSource) *report.AnalysisReport {
	var workerWg sync.WaitGroup
	sourceChan := make(chan source.InputSource, a.numWorkers)

	for i := 0; i < a.numWorkers; i++ {
		workerWg.Add(1)
		go a.worker(ctx, sourceChan, &workerWg)
	}

	go func() {
		defer close(sourceChan)
	feedLoop:
		for _, s := range sources {
			select {
			case sourceChan <- s:
			case <-ctx.Done():
				break feedLoop
			}
		}
	}()

	workerWg.Wait()
	return a.generateReport(sources, ctx.Err() != nil, a.ValidateOnly)
}

func (a *Analyser) worker(ctx context.Context, sourceChan <-chan source.InputSource, wg *sync.WaitGroup) {
	defer wg.Done()
	for src := range sourceChan {
		select {
		case <-ctx.Done():
			return
		default:
			a.processSource(ctx, src)
		}
	}
}

func (a *Analyser) processSource(ctx context.Context, src source.InputSource) {
	a.CurrentFolder.Store(src.Dir())
	reader, err := src.Open(ctx)
	if err != nil {
		log.Printf("Error opening source %q: %v\n", src.Path(), err)
		return
	}
	defer reader.Close()

	rowHasher := fnv.New64a()
	scanner := bufio.NewScanner(reader)
	const maxCapacity = 4 * 1024 * 1024
	buf := make([]byte, maxCapacity)
	scanner.Buffer(buf, maxCapacity)

	lineNumber := 0
	dir := src.Dir()
	for scanner.Scan() {
		if lineNumber%1000 == 0 {
			select {
			case <-ctx.Done():
				return
			default:
			}
		}
		lineNumber++
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		a.TotalRows.Add(1)
		a.rowsProcessedMutex.Lock()
		a.rowsProcessedPerFolder[dir]++
		a.rowsProcessedMutex.Unlock()

		var data report.JSONData
		if err := json.Unmarshal(line, &data); err != nil {
			log.Printf("Error decoding JSON on line %d in source %q: %v\n", lineNumber, src.Path(), err)
			continue
		}
		a.processRow(data, src.Path(), lineNumber, rowHasher)
	}
	if err := scanner.Err(); err != nil {
		log.Printf("Scanner error in source %q: %v\n", src.Path(), err)
		return
	}

	a.processedPathsMutex.Lock()
	a.processedPaths[src.Path()] = true
	a.processedPathsMutex.Unlock()
	a.ProcessedFiles.Add(1)
}

func (a *Analyser) processRow(data report.JSONData, filePath string, lineNumber int, rowHasher hash.Hash64) {
	if !a.checkKey {
		return
	}

	if _, ok := data[a.uniqueKey]; ok {
		dir := filepath.Dir(filePath)
		a.keysFoundMutex.Lock()
		a.keysFoundPerFolder[dir]++
		a.keysFoundMutex.Unlock()

		if a.ValidateOnly {
			return
		}

		idStr := fmt.Sprintf("%v", data[a.uniqueKey])
		loc := report.LocationInfo{FilePath: filePath, LineNumber: lineNumber}
		a.idMutex.Lock()
		a.idLocations[idStr] = append(a.idLocations[idStr], loc)
		a.idMutex.Unlock()
	}

	if a.checkRow && !a.ValidateOnly {
		rowHasher.Reset()
		compactRow, _ := json.Marshal(data)
		_, _ = rowHasher.Write(compactRow)
		hashString := strconv.FormatUint(rowHasher.Sum64(), 10)
		loc := report.LocationInfo{FilePath: filePath, LineNumber: lineNumber}
		a.rowMutex.Lock()
		a.rowHashes[hashString] = append(a.rowHashes[hashString], loc)
		a.rowMutex.Unlock()
	}
}

func (a *Analyser) generateReport(sources []source.InputSource, wasCancelled, isValidation bool) *report.AnalysisReport {
	rep := &report.AnalysisReport{
		DuplicateIDs:  make(map[string][]report.LocationInfo),
		DuplicateRows: make(map[string][]report.LocationInfo),
	}
	totalIDs, uniqueDuplicateIDsCount := 0, 0
	dupeIDsPerFolder := make(map[string]int)

	if a.checkKey && !isValidation {
		for id, locations := range a.idLocations {
			totalIDs += len(locations)
			if len(locations) > 1 {
				uniqueDuplicateIDsCount++
				rep.DuplicateIDs[id] = locations
				for _, loc := range locations {
					dupeIDsPerFolder[filepath.Dir(loc.FilePath)]++
				}
			}
		}
	}
	totalDuplicateRowsCount := 0
	dupeRowsPerFolder := make(map[string]int)
	if a.checkRow && !isValidation {
		for hash, locations := range a.rowHashes {
			if len(locations) > 1 {
				totalDuplicateRowsCount += len(locations)
				rep.DuplicateRows[hash] = locations
				for _, loc := range locations {
					dupeRowsPerFolder[filepath.Dir(loc.FilePath)]++
				}
			}
		}
	}

	folderDetails := make(map[string]report.FolderDetail)
	totalOverallBytes := int64(0)
	totalKeysFound := 0

	a.processedPathsMutex.Lock()
	defer a.processedPathsMutex.Unlock()

	for _, s := range sources {
		dir := s.Dir()
		detail := folderDetails[dir]
		size := s.Size()

		detail.TotalFiles++
		detail.TotalSizeBytes += size
		totalOverallBytes += size

		if a.processedPaths[s.Path()] {
			detail.FilesProcessed++
			detail.ProcessedSizeBytes += size
		}
		detail.KeysFound = int(a.keysFoundPerFolder[dir])
		detail.RowsProcessed = int(a.rowsProcessedPerFolder[dir])
		folderDetails[dir] = detail
	}

	processedCount := a.ProcessedFiles.Load()
	processedBytes := int64(0)
	for _, detail := range folderDetails {
		processedBytes += detail.ProcessedSizeBytes
		totalKeysFound += detail.KeysFound
	}

	if isValidation {
		totalIDs = totalKeysFound
	}

	rowCount := a.TotalRows.Load()
	avgRows := 0.0
	if processedCount > 0 {
		avgRows = float64(rowCount) / float64(processedCount)
	}

	uniqueFolders := make(map[string]bool)
	for _, s := range sources {
		uniqueFolders[s.Dir()] = true
	}
	avgFilesPerFolder := 0.0
	if len(uniqueFolders) > 0 {
		avgFilesPerFolder = float64(len(sources)) / float64(len(uniqueFolders))
	}

	rep.Summary = report.SummaryReport{
		IsValidationReport:        isValidation,
		IsPartialReport:           wasCancelled,
		FilesProcessed:            processedCount,
		TotalFiles:                len(sources),
		ProcessedDataSizeBytes:    processedBytes,
		TotalDataSizeOverallBytes: totalOverallBytes,
		ProcessedDataSizeHuman:    report.HumanSize(processedBytes),
		TotalDataSizeOverallHuman: report.HumanSize(totalOverallBytes),
		TotalRowsProcessed:        rowCount,
		UniqueKey:                 a.uniqueKey,
		TotalKeyOccurrences:       totalIDs,
		UniqueKeysDuplicated:      uniqueDuplicateIDsCount,
		DuplicateRowInstances:     totalDuplicateRowsCount,
		AverageRowsPerFile:        avgRows,
		AverageFilesPerFolder:     avgFilesPerFolder,
		DuplicateIDsPerFolder:     dupeIDsPerFolder,
		DuplicateRowsPerFolder:    dupeRowsPerFolder,
		FolderDetails:             folderDetails,
	}
	return rep
}
