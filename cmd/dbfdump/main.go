package main

import (
	"encoding/csv"
	"log"
	"os"

	"github.com/tadvi/dbf"
)

func main() {
	if len(os.Args) < 2 {
		log.Fatal("Missing dbf file name as first parameter")

		log.Println("\nUsage:")
		log.Println("    dbfdump input.dbf [output.csv]\n")
	}
	dbffile := os.Args[1]
	csvfile := "output.csv"
	if len(os.Args) > 2 {
		csvfile = os.Args[2]
	}

	save(dbffile, csvfile)
}

func save(dbffile, csvfile string) {
	db, err := dbf.LoadFile(dbffile)
	if err != nil {
		log.Fatal(err)
	}

	fl, err := os.Create(csvfile)
	if err != nil {
		log.Fatal(err)
	}
	defer fl.Close()
	w := csv.NewWriter(fl)
	defer w.Flush()

	header := []string{}
	for _, field := range db.Fields() {
		header = append(header, field.Name)
	}
	if err := w.Write(header); err != nil {
		log.Fatal(err)
	}

	// once we have both CSV and DBF open, write CSV while iterating rows
	var count int
	iter := db.NewIterator()
	for iter.Next() {
		arr := iter.Row()
		if err := w.Write(arr); err != nil {
			log.Fatal(err)
		}
		count++
	}
	log.Println("Total records in CSV:", count)
}
