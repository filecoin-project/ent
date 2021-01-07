package lib

import (
	"fmt"
	"io"
	"log"

	"github.com/filecoin-project/go-state-types/rt"
)

type MigrationLogger struct {
	l *log.Logger
}

func NewMigrationLogger(out io.Writer) *MigrationLogger {
	return &MigrationLogger{
		l: log.New(out, "~ent~", 0),
	}
}

func (m *MigrationLogger) Log(level rt.LogLevel, msg string, args ...interface{}) {
	var prefix string
	if level == rt.DEBUG {
		prefix = "[DEBUG]"
	} else if level == rt.INFO {
		prefix = "[INFO]"
	} else if level == rt.WARN {
		prefix = "[WARN]"
	} else {
		prefix = "[ERROR]"
	}
	outStr := fmt.Sprintf("%s %s", prefix, msg)
	m.l.Printf(outStr, args...)
}
