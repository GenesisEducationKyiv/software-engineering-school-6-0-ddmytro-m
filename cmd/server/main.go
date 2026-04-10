package main

import (
	"time"

	"github.com/ddmytro-m/github-scanner/internal/infra/db"
)

func main() {
	_ = db.Get()
	defer db.Close()

	time.Sleep(10 * time.Second)
}
