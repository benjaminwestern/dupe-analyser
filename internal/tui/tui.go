// internal/tui/tui.go
package tui

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"cloud.google.com/go/storage"
	"github.com/charmbracelet/bubbles/progress"
	"github.com/charmbracelet/bubbles/spinner"
	"github.com/charmbracelet/bubbles/textinput"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"

	"github.com/benjaminwestern/dupe-analyser/internal/analyser"
	"github.com/benjaminwestern/dupe-analyser/internal/config"
	"github.com/benjaminwestern/dupe-analyser/internal/report"
	"github.com/benjaminwestern/dupe-analyser/internal/source"
)

const (
	viewMenu int = iota
	viewOptions
	viewHelp
	viewInputPath
	viewInputKey
	viewInputLogPath
	viewProcessing
	viewCancelling
	viewReport
	viewPurgeSelection
	viewPurging
)

var (
	spinnerStyle    = lipgloss.NewStyle().Foreground(lipgloss.Color("63"))
	statusStyle     = lipgloss.NewStyle().MarginLeft(1)
	helpStyle       = lipgloss.NewStyle().Foreground(lipgloss.Color("240")).Margin(1, 0)
	timingStyle     = lipgloss.NewStyle().Foreground(lipgloss.Color("240"))
	errorStyle      = lipgloss.NewStyle().Foreground(lipgloss.Color("196"))
	headerStyle     = lipgloss.NewStyle().Bold(true).MarginBottom(1).Underline(true)
	menuCursorStyle = lipgloss.NewStyle().Foreground(lipgloss.Color("212"))
	selectionStyle  = lipgloss.NewStyle().Foreground(lipgloss.Color("202"))
	reportStyle     = lipgloss.NewStyle().Padding(0, 1).Border(lipgloss.RoundedBorder()).BorderForeground(lipgloss.Color("63"))
)

type sourcesFoundMsg struct{ sources []source.InputSource }
type progressUpdateMsg struct{}
type allWorkCompleteMsg struct{ report *report.AnalysisReport; savedFilenameBase string }
type purgeResultMsg struct {
	filesModified  int
	recordsDeleted int
	err            error
}
type errMsg struct{ err error }

type model struct {
	ctx context.Context

	jobCtx          context.Context
	jobCancel       context.CancelFunc
	wasCancelled    bool
	processing      bool
	analyser        *analyser.Analyser
	originalSources []source.InputSource
	isValidationRun bool
	
	viewState       int
	quitting        bool
	err             error
	status          string
	wantsToRestart  bool
	wantsToStartNew bool
	gcsAvailable    bool
	width           int
	height          int

	pathInput    textinput.Model
	keyInput     textinput.Model
	logPathInput textinput.Model
	spinner      spinner.Model
	progress     progress.Model
	
	startTime        time.Time
	totalElapsedTime time.Duration
	eta              time.Duration
	finalReport      *report.AnalysisReport
	savedFilename    string
	
	path                string
	key                 string
	workers             int
	logPath             string
	checkKey            bool
	checkRow            bool
	showFolderBreakdown bool
	outputTxt           bool
	outputJson          bool
	purgeIds            bool
	purgeRows           bool

	menuCursor    int
	optionsCursor int

	purgeIDKeys          []string
	purgeRowHashes       []string
	purgeCursor          int
	purgeSelectionCursor int
	recordsToDelete      map[string]map[int]bool
	purgeStats           purgeResultMsg
}

func testGCSClient() bool {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	client, err := storage.NewClient(ctx)
	if err != nil {
		log.Printf("GCS client pre-flight check failed: %v. GCS functionality will be disabled.", err)
		return false
	}
	client.Close()
	return true
}

func Run(cfg *config.Config) (*config.Config, bool, bool, error) {
	cfg.GCSAvailable = testGCSClient()
	ctx := context.Background()
	m, err := initModel(ctx, cfg)
	if err != nil {
		return nil, false, false, fmt.Errorf("failed to initialise TUI model: %w", err)
	}

	p := tea.NewProgram(m, tea.WithAltScreen())
	finalModel, err := p.Run()
	if err != nil {
		return nil, false, false, fmt.Errorf("error running TUI: %w", err)
	}

	fm, ok := finalModel.(model)
	if !ok {
		return nil, false, false, fmt.Errorf("could not cast final model")
	}

	return fm.buildConfig(), fm.wantsToRestart, fm.wantsToStartNew, nil
}

func initModel(ctx context.Context, cfg *config.Config) (model, error) {
	pathInput := textinput.New()
	if cfg.GCSAvailable {
		pathInput.Placeholder = "/path/a,/path/b,gs://bucket/c"
	} else {
		pathInput.Placeholder = "/path/a,/path/b (GCS unavailable)"
	}
	pathInput.Focus()
	pathInput.SetValue(cfg.Path)

	keyInput := textinput.New()
	keyInput.Placeholder = "id"
	keyInput.SetValue(cfg.Key)

	logPathInput := textinput.New()
	logPathInput.SetValue(cfg.LogPath)

	s := spinner.New()
	s.Spinner = spinner.Dot
	s.Style = spinnerStyle
	p := progress.New(progress.WithDefaultGradient())

	m := model{
		ctx:             ctx,
		pathInput:       pathInput,
		keyInput:        keyInput,
		logPathInput:    logPathInput,
		spinner:         s,
		progress:        p,
		recordsToDelete: make(map[string]map[int]bool),
		viewState:       viewMenu,
		gcsAvailable:    cfg.GCSAvailable,

		path:                cfg.Path,
		key:                 cfg.Key,
		workers:             cfg.Workers,
		logPath:             cfg.LogPath,
		checkKey:            cfg.CheckKey,
		checkRow:            cfg.CheckRow,
		showFolderBreakdown: cfg.ShowFolderBreakdown,
		outputTxt:           cfg.EnableTxtOutput,
		outputJson:          cfg.EnableJsonOutput,
		purgeIds:            cfg.PurgeIDs,
		purgeRows:           cfg.PurgeRows,
	}

	if m.path != "" {
		m.viewState = viewProcessing
	}

	return m, nil
}

func (m model) Init() tea.Cmd {
	if m.viewState == viewProcessing {
		paths := strings.Split(m.path, ",")
		for _, p := range paths {
			if strings.HasPrefix(strings.TrimSpace(p), "gs://") && !m.gcsAvailable {
				m.viewState = viewInputPath
				m.err = fmt.Errorf("cannot process GCS path: GCS credentials not available")
				return nil
			}
		}
		return discoverAllSourcesCmd(m.ctx, paths)
	}
	return textinput.Blink
}

func (m *model) buildConfig() *config.Config {
	return &config.Config{
		Path:                m.path,
		Key:                 m.key,
		Workers:             m.workers,
		LogPath:             m.logPath,
		CheckKey:            m.checkKey,
		CheckRow:            m.checkRow,
		ShowFolderBreakdown: m.showFolderBreakdown,
		EnableTxtOutput:     m.outputTxt,
		EnableJsonOutput:    m.outputJson,
		PurgeIDs:            m.purgeIds,
		PurgeRows:           m.purgeRows,
	}
}

func saveConfigCmd(cfg *config.Config) tea.Cmd {
	return func() tea.Msg {
		if err := cfg.Save(); err != nil {
			log.Printf("Failed to save config: %v", err)
		}
		return nil
	}
}

func (m model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	var cmd tea.Cmd

	if m.quitting {
		return m, tea.Quit
	}

	switch msg := msg.(type) {
	case tea.KeyMsg:
		if m.err != nil {
			m.err = nil
			m.viewState = viewMenu
			return m, nil
		}
		if msg.String() == "ctrl+c" || msg.String() == "q" {
			if m.viewState == viewProcessing {
				m.status = "Cancelling... generating partial report."
				m.viewState = viewCancelling
				m.wasCancelled = true
				if !m.startTime.IsZero() {
					m.totalElapsedTime += time.Since(m.startTime)
					m.startTime = time.Time{}
				}
				if m.jobCancel != nil {
					m.jobCancel()
				}
				return m, nil
			}
			if m.viewState == viewCancelling || m.viewState == viewPurging {
				return m, nil
			}
			m.quitting = true
			if m.jobCancel != nil {
				m.jobCancel()
			}
			return m, tea.Quit
		}
		if msg.Type == tea.KeyEsc {
			switch m.viewState {
			case viewHelp, viewOptions, viewInputPath, viewReport:
				m.viewState = viewMenu
				return m, nil
			case viewInputKey:
				m.viewState = viewInputPath
				m.keyInput.Blur()
				m.pathInput.Focus()
				return m, textinput.Blink
			case viewInputLogPath:
				m.viewState = viewOptions
				m.logPathInput.Blur()
				return m, nil
			case viewPurgeSelection:
				m.viewState = viewReport
				m.purgeCursor = 0
				m.purgeSelectionCursor = 0
				m.recordsToDelete = make(map[string]map[int]bool)
				m.purgeIDKeys = nil
				m.purgeRowHashes = nil
				return m, nil
			}
		}
	}

	switch m.viewState {
	case viewMenu:
		return updateMenu(m, msg)
	case viewOptions:
		return updateOptions(m, msg)
	case viewHelp:
		if _, ok := msg.(tea.KeyMsg); ok {
			m.viewState = viewMenu
		}
		return m, nil
	case viewInputPath:
		return updateInputPath(m, msg)
	case viewInputKey:
		return updateInputKey(m, msg)
	case viewInputLogPath:
		return updateInputLogPath(m, msg)
	case viewReport:
		return updateReport(m, msg)
	case viewPurgeSelection:
		return updatePurgeSelection(m, msg)
	}

	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width = msg.Width
		m.height = msg.Height
		m.progress.Width = msg.Width - 10
		if m.progress.Width > 120 {
			m.progress.Width = 120
		}
		return m, nil
	case sourcesFoundMsg:
		m.originalSources = msg.sources
		m.processing = true
		m.totalElapsedTime = 0
		m.startTime = time.Now()
		m.analyser = analyser.New(m.key, m.workers, m.checkKey, m.checkRow, m.isValidationRun)
		m.jobCtx, m.jobCancel = context.WithCancel(m.ctx)

		if m.isValidationRun {
			m.status = fmt.Sprintf("Found %d files. Validating key '%s'...", len(m.originalSources), m.key)
		} else {
			m.status = fmt.Sprintf("Found %d files. Analysing...", len(m.originalSources))
		}

		return m, tea.Batch(
			startAnalysisCmd(m.analyser, m.jobCtx, m.originalSources, m.logPath, m.outputTxt, m.outputJson, m.checkKey, m.checkRow, m.showFolderBreakdown),
			m.spinner.Tick,
			pollProgressCmd(&m),
		)
	case progressUpdateMsg:
		return updateProgress(m)
	case progress.FrameMsg:
		progressModel, cmd := m.progress.Update(msg)
		if newModel, ok := progressModel.(progress.Model); ok {
			m.progress = newModel
		}
		return m, cmd
	case spinner.TickMsg:
		if m.viewState == viewProcessing || m.viewState == viewCancelling || m.viewState == viewPurging {
			m.spinner, cmd = m.spinner.Update(msg)
			return m, cmd
		}
		return m, nil
	case allWorkCompleteMsg:
		m.progress.SetPercent(1.0)
		if !m.startTime.IsZero() {
			m.totalElapsedTime += time.Since(m.startTime)
			m.startTime = time.Time{}
		}
		msg.report.Summary.TotalElapsedTime = m.totalElapsedTime.Round(time.Second).String()
		m.finalReport = msg.report
		m.savedFilename = msg.savedFilenameBase
		m.viewState = viewReport
		return m, nil
	case purgeResultMsg:
		m.purgeStats = msg
		m.viewState = viewReport
		return m, nil
	case errMsg:
		m.err = msg.err
		if m.viewState == viewProcessing {
			m.viewState = viewMenu
		}
		return m, nil
	}
	return m, nil
}

func (m model) View() string {
	if m.quitting {
		return "Exiting...\n"
	}

	if m.err != nil {
		maxContentWidth := 80
		if contentWidth := m.width - 8; m.width > 0 && contentWidth < maxContentWidth {
			maxContentWidth = contentWidth
		}

		errorHeader := lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("196")).Render("An Error Occurred")
		errorBodyStyle := lipgloss.NewStyle().Width(maxContentWidth)
		errorBody := errorBodyStyle.Render(fmt.Sprintf("%v", m.err))
		helpText := helpStyle.Render("\nPress any key to return to the main menu.")

		content := lipgloss.JoinVertical(lipgloss.Left, errorHeader, "\n", errorBody, "\n", helpText)
		box := lipgloss.NewStyle().
			Border(lipgloss.NormalBorder(), true).
			BorderForeground(lipgloss.Color("240")).
			Padding(1, 2).
			Render(content)

		return lipgloss.Place(m.width, m.height, lipgloss.Center, lipgloss.Center, box)
	}

	switch m.viewState {
	case viewMenu:
		return renderMenu(&m)
	case viewOptions:
		return renderOptions(&m)
	case viewHelp:
		return renderHelp(&m)
	case viewInputPath:
		return renderInputPath(&m)
	case viewInputKey:
		return renderInputKey(&m)
	case viewInputLogPath:
		return renderInputLogPath(&m)
	case viewProcessing, viewCancelling:
		return renderProcessing(&m)
	case viewReport:
		return renderReport(&m)
	case viewPurgeSelection:
		return renderPurgeSelection(&m)
	case viewPurging:
		return fmt.Sprintf("\n%s %s\n", m.spinner.View(), m.status)
	}
	return ""
}

func discoverAllSourcesCmd(ctx context.Context, paths []string) tea.Cmd {
	return func() tea.Msg {
		sources, err := source.DiscoverAll(ctx, paths)
		if err != nil {
			if ctx.Err() == context.Canceled {
				return nil
			}
			return errMsg{err}
		}
		return sourcesFoundMsg{sources: sources}
	}
}

func startAnalysisCmd(a *analyser.Analyser, ctx context.Context, sources []source.InputSource, logPath string, outputTxt, outputJson, checkKey, checkRow, showFolderBreakdown bool) tea.Cmd {
	return func() tea.Msg {
		finalReport := a.Run(ctx, sources)
		if ctx.Err() == context.Canceled {
			if a.ProcessedFiles.Load() == 0 {
				return nil
			}
		}
		filenameBase := report.SaveAndLog(finalReport, logPath, outputTxt, outputJson, checkKey, checkRow, showFolderBreakdown)
		return allWorkCompleteMsg{report: finalReport, savedFilenameBase: filenameBase}
	}
}

func pollProgressCmd(m *model) tea.Cmd {
	return tea.Tick(time.Millisecond*100, func(t time.Time) tea.Msg {
		if m.analyser == nil {
			return progressUpdateMsg{}
		}
		if m.viewState != viewProcessing {
			return nil
		}
		return progressUpdateMsg{}
	})
}

func performPurgeCmd(recordsToDelete map[string]map[int]bool) tea.Cmd {
	return func() tea.Msg {
		backupDir := "deleted_records"
		if err := os.MkdirAll(backupDir, 0755); err != nil {
			return purgeResultMsg{err: fmt.Errorf("could not create backup dir: %w", err)}
		}
		result := purgeResultMsg{}
		for filePath, lineNumbersToDelete := range recordsToDelete {
			file, err := os.Open(filePath)
			if err != nil {
				log.Printf("Purge: Could not open %s: %v", filePath, err)
				continue
			}
			var newContent, backupContent strings.Builder
			scanner := bufio.NewScanner(file)
			lineNumber := 0
			for scanner.Scan() {
				lineNumber++
				if lineNumbersToDelete[lineNumber] {
					backupContent.WriteString(scanner.Text() + "\n")
					result.recordsDeleted++
				} else {
					newContent.WriteString(scanner.Text() + "\n")
				}
			}
			file.Close()
			if err := scanner.Err(); err != nil {
				log.Printf("Purge: Error scanning %s: %v", filePath, err)
				continue
			}
			if backupContent.Len() > 0 {
				backupFileName := fmt.Sprintf("deleted_records_%s", filepath.Base(filePath))
				backupPath := filepath.Join(backupDir, backupFileName)
				if err := os.WriteFile(backupPath, []byte(backupContent.String()), 0644); err != nil {
					log.Printf("Purge: Could not write backup for %s: %v", filePath, err)
					continue
				}
			}
			if err := os.WriteFile(filePath, []byte(newContent.String()), 0644); err != nil {
				log.Printf("Purge: Could not overwrite original file %s: %v", filePath, err)
				continue
			}
			result.filesModified++
		}
		return result
	}
}

func updateProgress(m model) (tea.Model, tea.Cmd) {
	if m.analyser == nil {
		return m, pollProgressCmd(&m)
	}
	processed := m.analyser.ProcessedFiles.Load()
	total := len(m.originalSources)
	percent := 0.0
	if total > 0 {
		percent = float64(processed) / float64(total)
		elapsed := m.totalElapsedTime + time.Since(m.startTime)
		if processed > 10 && percent < 1.0 {
			timePerFile := elapsed / time.Duration(processed)
			remainingFiles := total - int(processed)
			m.eta = time.Duration(remainingFiles) * timePerFile
		}
	}
	folderStr := "Discovering..."
	if f, ok := m.analyser.CurrentFolder.Load().(string); ok && f != "" {
		folderStr = f
	}
	m.status = fmt.Sprintf("Folder: %s | File %d of %d", folderStr, processed, total)
	var cmds []tea.Cmd
	cmds = append(cmds, m.progress.SetPercent(percent))
	if percent < 1.0 && m.viewState == viewProcessing {
		cmds = append(cmds, pollProgressCmd(&m))
	}
	return m, tea.Batch(cmds...)
}

func updateMenu(m model, msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.KeyMsg:
		switch msg.String() {
		case "up", "k":
			if m.menuCursor > 0 {
				m.menuCursor--
			}
		case "down", "j":
			if m.menuCursor < 3 {
				m.menuCursor++
			}
		case "?":
			m.viewState = viewHelp
		case "enter":
			m.analyser = nil
			m.finalReport = nil
			m.originalSources = nil
			m.err = nil

			switch m.menuCursor {
			case 0: // Start Validator
				m.isValidationRun = true
				m.viewState = viewInputPath
				m.pathInput.Focus()
				return m, textinput.Blink
			case 1: // Start Full Analysis
				m.isValidationRun = false
				m.viewState = viewInputPath
				m.pathInput.Focus()
				return m, textinput.Blink
			case 2: // Options
				m.viewState = viewOptions
			case 3: // Quit
				m.quitting = true
				return m, tea.Quit
			}
		}
	}
	return m, nil
}
func updateOptions(m model, msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.KeyMsg:
		switch msg.String() {
		case "up", "k":
			if m.optionsCursor > 0 {
				m.optionsCursor--
			}
		case "down", "j":
			if m.optionsCursor < 9 {
				m.optionsCursor++
			}
		case "left":
			if m.optionsCursor == 0 && m.workers > 1 {
				m.workers--
			}
		case "right":
			if m.optionsCursor == 0 {
				m.workers++
			}
		case "enter":
			switch m.optionsCursor {
			case 1:
				m.checkKey = !m.checkKey
			case 2:
				m.checkRow = !m.checkRow
			case 3:
				m.showFolderBreakdown = !m.showFolderBreakdown
			case 4:
				m.outputTxt = !m.outputTxt
			case 5:
				m.outputJson = !m.outputJson
			case 6:
				m.purgeIds = !m.purgeIds
			case 7:
				m.purgeRows = !m.purgeRows
			case 8:
				m.viewState = viewInputLogPath
				m.logPathInput.Focus()
				return m, textinput.Blink
			case 9:
				m.viewState = viewMenu
			}
			return m, saveConfigCmd(m.buildConfig())
		}
	}
	return m, nil
}

func updateInputPath(m model, msg tea.Msg) (tea.Model, tea.Cmd) {
	var cmd tea.Cmd
	switch msg := msg.(type) {
	case tea.KeyMsg:
		if msg.Type == tea.KeyEnter {
			m.path = m.pathInput.Value()
			if m.path == "" {
				m.err = fmt.Errorf("path cannot be empty")
				return m, nil
			}

			paths := strings.Split(m.path, ",")
			for i, p := range paths {
				paths[i] = strings.TrimSpace(p)
				if strings.HasPrefix(paths[i], "gs://") && !m.gcsAvailable {
					m.err = fmt.Errorf("cannot process GCS path: GCS credentials not available")
					return m, nil
				}
			}

			if m.isValidationRun || m.checkKey {
				m.viewState = viewInputKey
				m.pathInput.Blur()
				m.keyInput.Focus()
				return m, textinput.Blink
			}
			m.viewState = viewProcessing
			return m, discoverAllSourcesCmd(m.ctx, paths)
		}
	}
	m.pathInput, cmd = m.pathInput.Update(msg)
	return m, cmd
}

func updateInputKey(m model, msg tea.Msg) (tea.Model, tea.Cmd) {
	var cmd tea.Cmd
	switch msg := msg.(type) {
	case tea.KeyMsg:
		if msg.Type == tea.KeyEnter {
			m.key = m.keyInput.Value()
			if m.key == "" {
				m.err = fmt.Errorf("unique key cannot be empty")
				return m, nil
			}
			m.keyInput.Blur()
			m.viewState = viewProcessing
			paths := strings.Split(m.path, ",")
			for i, p := range paths {
				paths[i] = strings.TrimSpace(p)
			}
			return m, discoverAllSourcesCmd(m.ctx, paths)
		}
	}
	m.keyInput, cmd = m.keyInput.Update(msg)
	return m, cmd
}

func updateInputLogPath(m model, msg tea.Msg) (tea.Model, tea.Cmd) {
	var cmd tea.Cmd
	switch msg := msg.(type) {
	case tea.KeyMsg:
		if msg.Type == tea.KeyEnter {
			m.logPath = m.logPathInput.Value()
			if m.logPath == "" {
				m.err = fmt.Errorf("log path cannot be empty")
				return m, nil
			}
			m.logPathInput.Blur()
			m.viewState = viewOptions
			return m, saveConfigCmd(m.buildConfig())
		}
	}
	m.logPathInput, cmd = m.logPathInput.Update(msg)
	return m, cmd
}

func updateReport(m model, msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.KeyMsg:
		switch msg.String() {
		case "r":
			m.wantsToRestart = true
			return m, tea.Quit
		case "n":
			m.wantsToRestart = true
			m.wantsToStartNew = true
			return m, tea.Quit
		case "a":
			if m.finalReport != nil && m.finalReport.Summary.IsValidationReport {
				m.isValidationRun = false
				m.viewState = viewProcessing
				m.totalElapsedTime = 0
				m.wasCancelled = false
				return m, discoverAllSourcesCmd(m.ctx, strings.Split(m.path, ","))
			}
		case "c":
			if m.wasCancelled && m.analyser != nil {
				unprocessedSources := m.analyser.GetUnprocessedSources(m.originalSources)
				if len(unprocessedSources) > 0 {
					m.status = fmt.Sprintf("Continuing analysis on %d remaining files...", len(unprocessedSources))
					m.viewState = viewProcessing
					m.wasCancelled = false
					m.startTime = time.Now()
					m.jobCtx, m.jobCancel = context.WithCancel(m.ctx)
					return m, tea.Batch(
						startAnalysisCmd(m.analyser, m.jobCtx, unprocessedSources, m.logPath, m.outputTxt, m.outputJson, m.checkKey, m.checkRow, m.showFolderBreakdown),
						m.spinner.Tick,
						pollProgressCmd(&m),
					)
				}
			}
		case "p":
			hasIdDupes := m.finalReport != nil && len(m.finalReport.DuplicateIDs) > 0
			hasRowDupes := m.finalReport != nil && len(m.finalReport.DuplicateRows) > 0
			canStartPurge := m.finalReport != nil && !m.finalReport.Summary.IsValidationReport &&
				((m.purgeIds && hasIdDupes) || (m.purgeRows && hasRowDupes))

			isGCS := strings.Contains(m.path, "gs://")
			if !isGCS && canStartPurge && m.purgeStats.filesModified == 0 {
				if m.purgeIds && hasIdDupes {
					for k := range m.finalReport.DuplicateIDs {
						m.purgeIDKeys = append(m.purgeIDKeys, k)
					}
					sort.Strings(m.purgeIDKeys)
				}
				if m.purgeRows && hasRowDupes {
					for k := range m.finalReport.DuplicateRows {
						m.purgeRowHashes = append(m.purgeRowHashes, k)
					}
					sort.Strings(m.purgeRowHashes)
				}
				m.viewState = viewPurgeSelection
			}
		}
	}
	return m, nil
}
func updatePurgeSelection(m model, msg tea.Msg) (tea.Model, tea.Cmd) {
	var locations []report.LocationInfo
	if m.purgeCursor < len(m.purgeIDKeys) {
		key := m.purgeIDKeys[m.purgeCursor]
		locations = m.finalReport.DuplicateIDs[key]
	} else {
		hash := m.purgeRowHashes[m.purgeCursor-len(m.purgeIDKeys)]
		locations = m.finalReport.DuplicateRows[hash]
	}
	switch msg := msg.(type) {
	case tea.KeyMsg:
		switch msg.String() {
		case "up", "k":
			if m.purgeSelectionCursor > 0 {
				m.purgeSelectionCursor--
			}
		case "down", "j":
			if m.purgeSelectionCursor < len(locations)-1 {
				m.purgeSelectionCursor++
			}
		case "enter":
			for i, loc := range locations {
				if i != m.purgeSelectionCursor {
					if _, ok := m.recordsToDelete[loc.FilePath]; !ok {
						m.recordsToDelete[loc.FilePath] = make(map[int]bool)
					}
					m.recordsToDelete[loc.FilePath][loc.LineNumber] = true
				}
			}
			m.purgeCursor++
			m.purgeSelectionCursor = 0
			totalToPurge := len(m.purgeIDKeys) + len(m.purgeRowHashes)
			if m.purgeCursor >= totalToPurge {
				m.viewState = viewPurging
				m.status = "Purging records..."
				return m, tea.Batch(performPurgeCmd(m.recordsToDelete), m.spinner.Tick)
			}
		}
	}
	return m, nil
}

func renderMenu(m *model) string {
	choices := []string{"Start Validator", "Start Full Analysis", "Options", "Quit"}
	s := "What would you like to do?\n\n"
	for i, choice := range choices {
		cursor := " "
		if m.menuCursor == i {
			cursor = ">"
		}
		s += fmt.Sprintf("%s %s\n", menuCursorStyle.Render(cursor), choice)
	}
	return s + helpStyle.Render("\nUse up/down arrows, Enter to select, ? for help, q to quit.")
}
func renderOptions(m *model) string {
	opts := []string{
		fmt.Sprintf("Number of Workers: %d", m.workers),
		fmt.Sprintf("Duplicate Key Check: %t", m.checkKey),
		fmt.Sprintf("Duplicate Row Check: %t", m.checkRow),
		fmt.Sprintf("Show Folder Breakdown: %t", m.showFolderBreakdown),
		fmt.Sprintf("Enable TXT Report:   %t", m.outputTxt),
		fmt.Sprintf("Enable JSON Report:  %t", m.outputJson),
		fmt.Sprintf("Purge Duplicate IDs: %t", m.purgeIds),
		fmt.Sprintf("Purge Duplicate Rows:%t", m.purgeRows),
		fmt.Sprintf("Log/Report Path:     %s", m.logPath),
		"Back to Main Menu",
	}
	s := "Configure Options:\n\n"
	for i, choice := range opts {
		cursor := " "
		if m.optionsCursor == i {
			cursor = ">"
		}
		s += fmt.Sprintf("%s %s\n", menuCursorStyle.Render(cursor), choice)
	}
	return s + helpStyle.Render("\nUse up/down arrows, left/right or enter to toggle/change values.\nPress Enter on Log/Report Path to edit.")
}

func renderHelp(m *model) string {
	var pathHelp string
	if m.gcsAvailable {
		pathHelp = "-path <p1,p2>      Comma-separated list of paths to analyse (local or GCS)."
	} else {
		pathHelp = "-path <p1,p2>      Comma-separated list of local paths to analyse."
	}

	return fmt.Sprintf(`
  Help & Command-Line Flags

  This tool analyses a directory of JSON/NDJSON files for duplicate data.
  It can process local files and, if you are authenticated, Google Cloud Storage objects.

  --- Interactive Controls ---
  - Arrows:         Navigate menus
  - Enter:          Select menu item or submit input
  - q / ctrl+c:     Quit the application
  - esc:            Go back to the previous menu.
  - ?:              Show this help screen (from main menu)
  - r:              Restart the last job (from report screen)
  - c:              Continue a cancelled job (from report screen)
  - n:              Start a new job (from report screen)
  - a:              Run full analysis (after a validation report)
  - p:              Proceed to purge duplicates (from report screen, local files only)

  --- Headless Mode Flags ---
  %s
  -key <name>         Key for uniqueness check (default "id").
  -workers <int>      Number of concurrent workers (default 8).
  -log-path <path>    Directory to save logs and reports (default "logs").
  -validate           Run a key validation test and exit (headless only).
  -check.key <bool>   Enable duplicate key check (default true).
  -check.row <bool>   Enable duplicate row check (default true).
  -output.txt <bool>  Enable .txt report output (default false).
  -output.json <bool> Enable .json report output (default false).
  -show.folders <bool> Show per-folder breakdown table in summary (default true).
  -purge-ids <bool>   Enable interactive purging (default false, interactive & local only).
  -purge-rows <bool>  Enable interactive purging (default false, interactive & local only).
  -headless           Run without TUI and print report to stdout.
  -output <txt|json>  Output format for headless mode (default "txt").
  `, pathHelp)
}

func renderInputPath(m *model) string {
	pad := strings.Repeat(" ", 2)
	var prompt string
	if m.gcsAvailable {
		prompt = "Please enter one or more comma-separated paths to analyse:"
	} else {
		prompt = "Please enter one or more comma-separated local paths to analyse:"
	}
	help := helpStyle.Render("Press Enter to submit, 'q' or 'ctrl+c' to quit, 'esc' to go back.")
	return fmt.Sprintf("\n%s%s\n\n%s%s\n\n%s", pad, prompt, pad, m.pathInput.View(), help)
}

func renderInputLogPath(m *model) string {
	pad := strings.Repeat(" ", 2)
	help := helpStyle.Render("Press Enter to submit, 'q' or 'ctrl+c' to quit, 'esc' to go back.")
	return fmt.Sprintf("\n%sPlease enter the path for logs and reports:\n\n%s%s\n\n%s", pad, pad, m.logPathInput.View(), help)
}

func renderInputKey(m *model) string {
	pad := strings.Repeat(" ", 2)
	help := helpStyle.Render("Press Enter to submit, 'q' or 'ctrl+c' to quit, 'esc' to go back.")
	return fmt.Sprintf("\n%sPaths: %s\n\n%sPlease enter the JSON key to check for uniqueness (e.g., id, product_sku):\n\n%s%s\n\n%s", pad, m.path, pad, pad, m.keyInput.View(), help)
}

func renderProcessing(m *model) string {
	pad := strings.Repeat(" ", 2)
	var progressView, timingView string
	if m.processing {
		progressView = "\n" + m.progress.View()
		elapsedStr := (m.totalElapsedTime + time.Since(m.startTime)).Round(time.Second).String()
		etaStr := m.eta.Round(time.Second).String()
		timingView = timingStyle.Render(fmt.Sprintf(" (Elapsed: %s, ETA: %s)", elapsedStr, etaStr))
	}
	status := statusStyle.Render(m.status)
	if m.viewState == viewCancelling {
		return fmt.Sprintf("\n%s%s %s\n", pad, m.spinner.View(), m.status)
	}
	return fmt.Sprintf("\n%s%s%s%s\n%s", pad, m.spinner.View(), status, timingView, progressView) + helpStyle.Render("\nPress 'q' or 'ctrl+c' to cancel.")
}

func renderReport(m *model) string {
	if m.finalReport == nil {
		return "Generating report..."
	}
	var b strings.Builder
	b.WriteString("\n" + m.finalReport.String(false, m.checkKey, m.checkRow, m.showFolderBreakdown))
	if m.purgeStats.filesModified > 0 || m.purgeStats.recordsDeleted > 0 {
		purgeSummary := fmt.Sprintf("Files Modified: %d\nRecords Deleted: %d (and backed up)", m.purgeStats.filesModified, m.purgeStats.recordsDeleted)
		b.WriteString("\n\n" + reportStyle.Render(purgeSummary))
	} else if m.purgeStats.err != nil {
		b.WriteString("\n\n" + errorStyle.Render("Purge failed: "+m.purgeStats.err.Error()))
	}
	if !m.finalReport.Summary.IsValidationReport && (m.outputTxt || m.outputJson) {
		var parts []string
		if m.outputTxt {
			parts = append(parts, ".txt")
		}
		if m.outputJson {
			parts = append(parts, ".json")
		}
		b.WriteString("\n\n" + fmt.Sprintf("Reports saved to files with extension(s): %s", m.savedFilename))
	}

	helpParts := []string{}
	if m.finalReport != nil && m.finalReport.Summary.IsValidationReport {
		helpParts = append(helpParts, "(a)nalyse now")
	}
	if m.wasCancelled {
		helpParts = append(helpParts, "(c)ontinue")
	}
	helpParts = append(helpParts, "(r)estart", "(n)ew job")

	hasIdDupesToPurge := m.purgeIds && m.finalReport != nil && len(m.finalReport.DuplicateIDs) > 0
	hasRowDupesToPurge := m.purgeRows && m.finalReport != nil && len(m.finalReport.DuplicateRows) > 0
	canDisplayPurge := m.finalReport != nil && !m.finalReport.Summary.IsValidationReport && (hasIdDupesToPurge || hasRowDupesToPurge)

	isGCS := strings.Contains(m.path, "gs://")
	if !isGCS && canDisplayPurge && m.purgeStats.filesModified == 0 {
		helpParts = append(helpParts, "(p)urge")
	}
	helpParts = append(helpParts, "(q)uit")

	b.WriteString("\n" + helpStyle.Render("Press "+strings.Join(helpParts, ", ")+"."))
	return b.String()
}
func renderPurgeSelection(m *model) string {
	var b strings.Builder
	var locations []report.LocationInfo
	var title string
	totalToPurge := len(m.purgeIDKeys) + len(m.purgeRowHashes)
	isPurgingIDs := m.purgeCursor < len(m.purgeIDKeys)
	if isPurgingIDs {
		key := m.purgeIDKeys[m.purgeCursor]
		locations = m.finalReport.DuplicateIDs[key]
		title = fmt.Sprintf("Duplicate ID '%s'", key)
	} else {
		hash := m.purgeRowHashes[m.purgeCursor-len(m.purgeIDKeys)]
		locations = m.finalReport.DuplicateRows[hash]
		title = fmt.Sprintf("Duplicate Row (hash %s...)", hash[:8])
	}
	b.WriteString(fmt.Sprintf("Resolving %d of %d duplicate sets...\n", m.purgeCursor+1, totalToPurge))
	b.WriteString(headerStyle.Render(title) + "\n\n")
	b.WriteString("Select the one record to KEEP:\n")
	for i, loc := range locations {
		cursor := "  "
		if i == m.purgeSelectionCursor {
			cursor = selectionStyle.Render("> ")
		}
		b.WriteString(fmt.Sprintf("%sFile: %s\n  Line: %d\n", cursor, loc.FilePath, loc.LineNumber))
	}
	b.WriteString(helpStyle.Render("\nUse up/down arrows to select. Enter to confirm and move to next set."))
	return b.String()
}
