package logging

import (
	"io/ioutil"
	"log"
	"os"
)

func init() {
	Infolog.SetOutput(ioutil.Discard)
}

var Infolog = log.New(os.Stderr, "[firebolt-go-sdk]", log.Ldate|log.Ltime|log.Lshortfile)
