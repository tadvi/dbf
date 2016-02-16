package main

import (
	"encoding/csv"
	"log"
	"os"
	"strconv"
	"strings"

	"github.com/tadvi/dbf"
)

type FieldType int

const (
	None FieldType = iota
	Alpha
	Bool
	Int
	Float
)

func main() {
	if len(os.Args) < 2 {
		log.Fatal("Missing csv file name as first parameter")

		log.Println("\nUsage:")
		log.Println("    dbfload input.csv [output.dbf [field#=equals_value]]\n")
	}

	csvfile := os.Args[1]
	dbffile := "output.dbf"
	if len(os.Args) > 2 {
		dbffile = os.Args[2]
	}

	equals := ""
	if len(os.Args) > 3 {
		equals = os.Args[3]
	}

	save(csvfile, dbffile, equals)
}

type FieldName struct {
	name     string
	typ      FieldType
	length   int
	truncate bool // truncated fields longer than 254
}

func save(csvfile, dbffile, equals string) {
	fl, err := os.Open(csvfile)
	if err != nil {
		log.Fatal(err)
	}
	defer fl.Close()
	r := csv.NewReader(fl)

	// we assume that first row contains field names
	records, err := r.ReadAll()
	if err != nil {
		log.Fatal(err)
	}
	// analyze data types
	uniq := map[string]bool{}
	isBool := map[string]bool{"T": true, "t": true, "F": true, "f": true, "y": true, "Y": true, "N": true, "n": true}
	names := []FieldName{}
	truncCount := 0

	// inference
	for row, rec := range records {
		for j, field := range rec {
			if row == 0 {
				if len(field) > 10 {
					log.Fatal("Field name can not be over 10 characters long ", field)
				}
				if uniq[field] {
					log.Fatal("Field names must be unique ", field)
				}
				uniq[field] = true
				names = append(names, FieldName{name: field, typ: None, length: 1})
				continue
			}

			if names[j].typ != Alpha {
				// analyze types
				if names[j].typ != Float {
					if _, err := strconv.ParseInt(field, 0, 64); err == nil {
						names[j].typ = Int
						continue
					}
				}
				if _, err := strconv.ParseFloat(field, 64); err == nil {
					names[j].typ = Float
					continue
				}
				if isBool[field] {
					names[j].typ = Bool
					continue
				}

				// at this point - must be Alpha
				names[j].typ = Alpha
			}

			if len(field) > 254 {
				truncCount++
			}

			if len(field) > 254 && !names[j].truncate {
				log.Println("Field is longer than 254 characters, and will be truncated '", names[j].name, "'")
				names[j].truncate = true
				records[row][j] = records[row][j][:250] + "..."
			}
			if names[j].length < len(field) {
				names[j].length = len(field)
				if len(field) > 254 {
					names[j].length = 254
				}
			}
		}
	}
	if truncCount > 0 {
		log.Println("Number of truncated fields:", truncCount, "\n")
	}

	db := dbf.New()
	log.Println("Creating table:")
	log.Println("------------------------")

	for _, f := range names {
		switch f.typ {
		case None, Alpha:
			db.AddTextField(f.name, uint8(f.length))
			log.Println("Text field:", f.name, "size:", f.length)
		case Bool:
			db.AddBoolField(f.name)
			log.Println("Bool field:", f.name)
		case Int:
			db.AddIntField(f.name)
			log.Println("Int field:", f.name)
		case Float:
			db.AddFloatField(f.name)
			log.Println("Float field:", f.name)
		}
	}
	log.Println("------------------------")

	equalsFNum, equalsVal, filterCount := -1, "", 0
	if equals != "" {
		arr := strings.Split(equals, "=")
		equalsFNum, err = strconv.Atoi(arr[0])
		if err != nil {
			log.Fatal("filter should be a field number, instead it is ", arr[0])
		}
		equalsVal = arr[1]
	}

	count := 0
	for row, rec := range records {
		if row == 0 {
			continue
		}
		if equalsFNum != -1 {
			if equalsFNum > len(rec)-1 {
				log.Fatal("filter field number outside of record length bounds: ", equalsFNum)
			}
			if rec[equalsFNum] != equalsVal {
				filterCount++
				continue
			}
		}

		n := db.AddRecord()
		count++

		for j, field := range rec {
			db.SetFieldValue(n, j, field)
		}
	}
	log.Println("Filtered records:", filterCount)
	log.Println("Total records loaded:", count)

	if err := db.SaveFile(dbffile); err != nil {
		log.Fatal(err)
	}
}
