package main

import (
	"fmt"
	"github.com/tadvi/dbf"
)

type Note struct {
	Name   string `dbf:"60"`
	Amount float64
}

func main() {
	db := dbf.New()
	if err := db.Create(Note{}); err != nil {
		panic(err)
	}

	db.Append(Note{"Tom", 444})
	db.Append(Note{"Bob", 555})
	db.Append(Note{"Stan", 777})
	err := db.SaveFile("temp.dbf")
	if err != nil {
		panic(err)
	}

	db, err = dbf.LoadFile("temp.dbf")
	if err != nil {
		panic(err)
	}

	iter1 := db.NewIterator()
	for iter1.Next() {
		n := new(Note)
		iter1.Read(n)
		n.Amount = 12
		iter1.Write(n)
		fmt.Println(*n)
	}

	iter := db.NewIterator()
	for iter.Next() {
		n := new(Note)
		iter.Read(n)
		fmt.Println(*n)
	}

	err = db.SaveFile("temp.dbf")
	if err != nil {
		panic(err)
	}
}
