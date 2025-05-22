package logging

import (
	"fmt"
	"os"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var Logger *zap.Logger

func ClearLogFile(logPath string) error {
	return os.WriteFile(logPath, []byte{}, 0644)
}

func InitLogger(level string, logfile string) {
	fmt.Printf("日志文件%v\n", logfile)
	ClearLogFile(logfile)

	var zapLevel zapcore.Level
	switch level {
	case "debug":
		zapLevel = zapcore.DebugLevel
	case "info":
		zapLevel = zapcore.InfoLevel
	case "warn":
		zapLevel = zapcore.WarnLevel
	case "error":
		zapLevel = zapcore.ErrorLevel
	default:
		zapLevel = zapcore.InfoLevel
	}

	encoderCfg := zapcore.EncoderConfig{
		TimeKey:        "", // 不显示时间
		LevelKey:       "level",
		NameKey:        "",
		CallerKey:      "", // 不显示 caller
		MessageKey:     "msg",
		StacktraceKey:  "",
		LineEnding:     zapcore.DefaultLineEnding,
		EncodeLevel:    zapcore.CapitalLevelEncoder, // INFO/WARN/ERROR 等大写
		EncodeTime:     nil,
		EncodeDuration: nil,
		EncodeCaller:   nil,
	}

	encoder := zapcore.NewConsoleEncoder(encoderCfg)

	// 打开日志文件
	logFile, err := os.OpenFile(logfile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		panic(err)
	}

	// 将 stdout 和 logfile 组合
	writeSyncer := zapcore.NewMultiWriteSyncer(
		zapcore.AddSync(os.Stdout),
		zapcore.AddSync(logFile),
	)

	core := zapcore.NewCore(encoder, writeSyncer, zapLevel)

	Logger = zap.New(core)

	// encoderCfg.TimeKey = "ts"
	// encoderCfg.EncodeTime = zapcore.ISO8601TimeEncoder
	// encoderCfg.CallerKey = "caller"
	// encoderCfg.EncodeCaller = zapcore.ShortCallerEncoder
	// encoderCfg.LevelKey = "level"
	// encoderCfg.MessageKey = "msg"

	// fileWriter := zapcore.AddSync(&lumberjack.Logger{
	// 	Filename:   logfile,
	// 	MaxSize:    100, // MB
	// 	MaxBackups: 10,
	// 	MaxAge:     30, // days
	// 	Compress:   true,
	// })

	// core := zapcore.NewCore(
	// 	zapcore.NewJSONEncoder(encoderCfg),
	// 	zapcore.NewMultiWriteSyncer(zapcore.AddSync(os.Stdout), fileWriter),
	// 	zapLevel,
	// )

	// Logger = zap.New(core, zap.AddCaller())
}
