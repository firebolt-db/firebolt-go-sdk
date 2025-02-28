package logging

import (
	"log"
	"os"
)

var Infolog = log.New(os.Stderr, "[firebolt-go-sdk]", log.Ldate|log.Ltime|log.Lshortfile)
