package main

import (
	"io"
	"log"
	"os"
)

type Result interface {
	Print(io io.WriteCloser)
}

type UniqueUsers []string

type kpi struct {
	SumAmount int
	NumUsers  int

	uniqueUsers UniqueUsers
}

type result map[int]kpi

func logic(pathUsers, pathTransactions string, skipHeader bool) (Result, error) {
	panic("todo")
}

func main() {
	baseDir := os.Getenv("BASE_DIR")
	if baseDir == "" {
		baseDir = "/fixtures"
	}

	results, err := logic(baseDir+"/users.csv", baseDir+"/transactions.csv", true)
	if err != nil {
		log.Fatalln(err)
	}

	results.Print(os.Stdout)
}
