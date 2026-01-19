package main

import (
	"log"

	"github.com/tom-ok1/proglog/internal/server"
)

func main() {
	srv := server.NewHTTPServer("0.0.0.0:8080")
	defer srv.Close()

	log.Printf("server is listening...\n")
	log.Fatal(srv.ListenAndServe())
}
