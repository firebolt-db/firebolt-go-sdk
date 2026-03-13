package logging

import (
	"io"
	"log"
	"os"
)

func init() {
	Infolog.SetOutput(io.Discard)
}

var Infolog = log.New(os.Stderr, "[firebolt-go-sdk]", log.Ldate|log.Ltime|log.Lshortfile)
var Errorlog = log.New(os.Stderr, "[firebolt-go-sdk][ERROR]", log.Ldate|log.Ltime|log.Lshortfile)
