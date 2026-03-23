package log

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/natefinch/lumberjack.v2"

	"github.com/SisyphusSQ/my2sql/internal/utils/timeutil"
)

// OutputMode 定义日志输出目标。
type OutputMode string

const (
	OutputStdio  OutputMode = "stdio"
	OutputStderr OutputMode = "stderr"
	OutputFile   OutputMode = "file"
)

// Logger 对外保持兼容的包级日志入口。
var Logger = mustNewDefaultLogger()

// New 创建并替换全局 logger。
func New(isDebug bool, mode OutputMode, filePath ...string) error {
	logger, err := newLogger(isDebug, mode, filePath...)
	if err != nil {
		return err
	}

	Logger = logger
	return nil
}

func mustNewDefaultLogger() *ZapLogger {
	logger, err := newLogger(true, OutputStdio)
	if err != nil {
		panic(err)
	}

	return logger
}

func newLogger(isDebug bool, mode OutputMode, filePath ...string) (*ZapLogger, error) {
	logLevel := zapcore.InfoLevel
	if isDebug {
		logLevel = zapcore.DebugLevel
	}

	writeSyncer, err := buildWriteSyncer(mode, filePath...)
	if err != nil {
		return nil, err
	}

	encoderCfg := zapcore.EncoderConfig{
		TimeKey:        "ts",
		LevelKey:       "level",
		NameKey:        "logger",
		CallerKey:      "caller",
		FunctionKey:    zapcore.OmitKey,
		MessageKey:     "msg",
		StacktraceKey:  "stacktrace",
		LineEnding:     zapcore.DefaultLineEnding,
		EncodeLevel:    customLevelEncoder,
		EncodeTime:     zapcore.TimeEncoderOfLayout(timeutil.TimeLayout),
		EncodeDuration: zapcore.SecondsDurationEncoder,
		EncodeCaller:   customCallerEncoder,
	}

	encoder := zapcore.NewConsoleEncoder(encoderCfg)
	core := zapcore.NewCore(encoder, writeSyncer, logLevel)
	logger := zap.New(core, zap.AddCaller(), zap.AddCallerSkip(1)).Sugar()

	return NewZapLogger(logger), nil
}

func buildWriteSyncer(mode OutputMode, filePath ...string) (zapcore.WriteSyncer, error) {
	switch mode {
	case OutputStdio:
		return zapcore.AddSync(os.Stdout), nil
	case OutputStderr:
		return zapcore.AddSync(os.Stderr), nil
	case OutputFile:
		logPath := defaultLogPath(filePath...)
		if err := os.MkdirAll(filepath.Dir(logPath), 0o755); err != nil {
			return nil, fmt.Errorf("create log directory: %w", err)
		}

		rotating := &lumberjack.Logger{
			Filename:   logPath,
			MaxSize:    100,
			MaxBackups: 3,
			MaxAge:     28,
			Compress:   true,
		}
		return zapcore.AddSync(rotating), nil
	default:
		return nil, fmt.Errorf("unsupported log output mode: %s", mode)
	}
}

func defaultLogPath(filePath ...string) string {
	if len(filePath) == 0 {
		return "./logs/app.log"
	}

	candidate := strings.TrimSpace(filePath[0])
	if candidate == "" {
		return "./logs/app.log"
	}

	return candidate
}

func customLevelEncoder(level zapcore.Level, enc zapcore.PrimitiveArrayEncoder) {
	enc.AppendString("[" + level.CapitalString() + "]")
}

func customCallerEncoder(caller zapcore.EntryCaller, enc zapcore.PrimitiveArrayEncoder) {
	if caller.Defined {
		enc.AppendString("[" + caller.TrimmedPath() + "]")
		return
	}

	enc.AppendString("[undefined]")
}

// ZapLogger 封装 zap 并兼容当前仓库的日志调用方式。
type ZapLogger struct {
	logger *zap.SugaredLogger
}

// NewZapLogger 构造适配器。
func NewZapLogger(logger *zap.SugaredLogger) *ZapLogger {
	return &ZapLogger{logger: logger}
}

// Debug 输出 Debug 日志。
func (l *ZapLogger) Debug(format string, args ...any) {
	l.logger.Debug(formatMessage(format, args...))
}

// Info 输出 Info 日志。
func (l *ZapLogger) Info(format string, args ...any) {
	l.logger.Info(formatMessage(format, args...))
}

// Warn 输出 Warn 日志。
func (l *ZapLogger) Warn(format string, args ...any) {
	l.logger.Warn(formatMessage(format, args...))
}

// Error 输出 Error 日志。
func (l *ZapLogger) Error(format string, args ...any) {
	l.logger.Error(formatMessage(format, args...))
}

// Fatal 输出 Fatal 日志并退出进程。
func (l *ZapLogger) Fatal(format string, args ...any) {
	l.logger.Fatal(formatMessage(format, args...))
}

// Sync 尝试刷新底层缓冲。
func (l *ZapLogger) Sync() {
	_ = l.logger.Sync()
}

func formatMessage(format string, args ...any) string {
	if len(args) == 0 {
		return format
	}

	if strings.Contains(format, "%") {
		return fmt.Sprintf(format, args...)
	}

	return fmt.Sprint(append([]any{format}, args...)...)
}
