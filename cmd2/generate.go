package main

import (
	"flag"
	"log"
	"os"

	"github.com/vesoft-inc/nebula-importer/pkg/cipher"
)

var secret = flag.String("secret", "", "param for secret")
var msg = flag.String("msg", "", "param for msg")

func main() {
	errCode := 0

	flag.Parse()
	if len(*secret) < 1 {
		log.Printf("Please defined the param for secret.")
		os.Exit(errCode)
	}

	if len(*msg) < 1 {
		log.Printf("Please defined the param for msg.")
		os.Exit(errCode)
	}

	encMsg := cipher.Aes256Encrypt(*msg, *secret)
	log.Printf("The encrypted information is [%s]", encMsg)
}
