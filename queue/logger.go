package queue

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
)

// DailyRotateWriter 按天分割日志的 io.Writer 实现。
// 每次写入前检查日期是否变化，若变化则自动切换到新文件。
// 同时支持清理超过指定天数的旧日志文件。
type DailyRotateWriter struct {
	mu      sync.Mutex
	dir     string   // 日志目录
	prefix  string   // 日志文件名前缀，如 "later"
	maxDays int      // 保留天数，0 表示不清理
	curDate string   // 当前日志文件的日期 (YYYY-MM-DD)
	curFile *os.File // 当前打开的日志文件句柄
}

// NewDailyRotateWriter 创建一个按天分割的日志写入器。
// dir: 日志文件存放目录
// prefix: 日志文件名前缀，如 "later" 生成 later-2026-06-16.log
// maxDays: 日志文件最大保留天数，超过此天数的文件将被清理。设为 0 则不自动清理。
func NewDailyRotateWriter(dir, prefix string, maxDays int) (*DailyRotateWriter, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("create log dir: %w", err)
	}
	w := &DailyRotateWriter{
		dir:     dir,
		prefix:  prefix,
		maxDays: maxDays,
	}
	// 初始化时立即打开当天的日志文件
	if err := w.rotate(); err != nil {
		return nil, err
	}
	// 清理过期日志
	if maxDays > 0 {
		w.cleanOldLogs()
	}
	return w, nil
}

// today 返回今天的日期字符串 YYYY-MM-DD
func (w *DailyRotateWriter) today() string {
	return time.Now().Format("2006-01-02")
}

// logPath 根据日期返回日志文件完整路径
func (w *DailyRotateWriter) logPath(date string) string {
	name := fmt.Sprintf("%s-%s.log", w.prefix, date)
	return filepath.Join(w.dir, name)
}

// rotate 切换到当天日期的日志文件
func (w *DailyRotateWriter) rotate() error {
	today := w.today()
	if w.curDate == today && w.curFile != nil {
		return nil // 无需切换
	}

	// 关闭旧文件
	if w.curFile != nil {
		w.curFile.Close()
	}

	// 打开新文件（追加模式）
	path := w.logPath(today)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("open log file %s: %w", path, err)
	}

	w.curDate = today
	w.curFile = f
	return nil
}

// Write 实现 io.Writer 接口。每次写入前检查日期是否变化。
func (w *DailyRotateWriter) Write(p []byte) (n int, err error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.rotate(); err != nil {
		return 0, err
	}

	return w.curFile.Write(p)
}

// Close 关闭当前日志文件
func (w *DailyRotateWriter) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.curFile != nil {
		err := w.curFile.Close()
		w.curFile = nil
		return err
	}
	return nil
}

// cleanOldLogs 清理超过 maxDays 的旧日志文件
func (w *DailyRotateWriter) cleanOldLogs() {
	cutoff := time.Now().AddDate(0, 0, -w.maxDays)
	pattern := filepath.Join(w.dir, fmt.Sprintf("%s-*.log", w.prefix))
	matches, err := filepath.Glob(pattern)
	if err != nil {
		return
	}
	for _, path := range matches {
		info, err := os.Stat(path)
		if err != nil {
			continue
		}
		if info.ModTime().Before(cutoff) {
			os.Remove(path)
		}
	}
}

// DailyRotateHook 是一个 logrus.Hook，将日志以 JSON 格式写入按天分割的文件。
// 控制台保持 TextFormatter 不变，仅文件使用 JSONFormatter。
type DailyRotateHook struct {
	writer   *DailyRotateWriter
	levels   []logrus.Level
	mu       sync.Mutex
	closed   bool
}

// NewDailyRotateHook 创建一个日志 Hook。
func NewDailyRotateHook(writer *DailyRotateWriter, levels []logrus.Level) *DailyRotateHook {
	if len(levels) == 0 {
		levels = logrus.AllLevels
	}
	return &DailyRotateHook{writer: writer, levels: levels}
}

// Levels 返回 Hook 关注的日志级别。
func (h *DailyRotateHook) Levels() []logrus.Level {
	return h.levels
}

// Fire 将日志条目格式化为 JSON 并写入文件。
func (h *DailyRotateHook) Fire(entry *logrus.Entry) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.closed {
		return nil
	}

	// 构造 JSON 格式，追加必要的字段
	data := make(logrus.Fields, len(entry.Data)+4)
	for k, v := range entry.Data {
		data[k] = v
	}
	data["time"] = entry.Time.Format("2006-01-02 15:04:05")
	data["level"] = entry.Level.String()
	data["msg"] = entry.Message

	// 如果有调用者信息，一并记录
	if entry.Caller != nil {
		data["file"] = fmt.Sprintf("%s:%d", entry.Caller.File, entry.Caller.Line)
		data["func"] = entry.Caller.Function
	}

	// 用 JSONFormatter 序列化
	formatter := &logrus.JSONFormatter{
		TimestampFormat: "2006-01-02 15:04:05",
	}
	line, err := formatter.Format(entry)
	if err != nil {
		return err
	}

	_, err = h.writer.Write(line)
	return err
}

// Close 关闭 Hook 对应的日志文件
func (h *DailyRotateHook) Close() error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.closed = true
	return h.writer.Close()
}

// InitLogger 初始化日志系统。
// 控制台输出保持 Text 格式，文件输出使用 JSON 格式并按天分割。
// logDir: 日志文件存放目录，为空则仅输出到控制台
// maxDays: 日志保留天数，<=0 表示默认 7 天
func InitLogger(logDir string, maxDays int) {
	if maxDays <= 0 {
		maxDays = 7
	}

	// 控制台保持 Text 格式，Formtter 已在 main.init 中设置
	logrus.SetOutput(os.Stdout)

	if logDir == "" {
		return
	}

	rotateWriter, err := NewDailyRotateWriter(logDir, "later", maxDays)
	if err != nil {
		fmt.Fprintf(os.Stderr, "init log file writer fail: %v, fallback to stdout only\n", err)
		return
	}

	hook := NewDailyRotateHook(rotateWriter, logrus.AllLevels)
	logrus.AddHook(hook)
}

// NopWriter 是一个不写任何内容的 Writer，防止第三方库向 stderr 写入
type NopWriter struct{}

func (NopWriter) Write(p []byte) (n int, err error) {
	return len(p), nil
}

var _ io.Writer = NopWriter{}
