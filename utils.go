package fireboltgosdk

import (
	"fmt"
	"log"
)

func ConstructNestedError(message string, err error) error {
	log.Printf("%s: %v", message, err)
	return fmt.Errorf("%s: %v", message, err)
}
