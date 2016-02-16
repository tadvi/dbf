package dbf

import (
	"os"
	"testing"
)

const tempdbf = "temp.dbf"

// TTable test table. Order of struct members is important.
type TTable struct {
	Boolean bool
	Text    string
	Int     int
	Float   float64
}

func TestNew(t *testing.T) {
	db := New()
	db.AddBoolField("boolean")
	db.AddTextField("text", 40)
	db.AddIntField("int")
	db.AddFloatField("float")

	addRecord(t, db)
	checkCount(t, db, 1)
	addStruct(t, db)
	checkCount(t, db, 2)
	addRecord(t, db)
	checkCount(t, db, 3)
	addStruct(t, db)
	checkCount(t, db, 4)
	addStruct(t, db)
	checkCount(t, db, 5)

	updateRecord(t, db, 3)
	checkCount(t, db, 5)
	delRecord(t, db, 4)
	checkCount(t, db, 4)

	if err := db.SaveFile(tempdbf); err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tempdbf)

	dbload, err := LoadFile(tempdbf)
	if err != nil {
		t.Fatal(err)
	}
	checkCount(t, dbload, 4)
	addStruct(t, db)
	checkCount(t, db, 5)
}

func TestNewStruct(t *testing.T) {
	db := New()
	err := db.Create(TTable{})
	if err != nil {
		t.Fatal(err)
	}

	addRecord(t, db)
	checkCount(t, db, 1)
	addStruct(t, db)
	checkCount(t, db, 2)
	addRecord(t, db)
	checkCount(t, db, 3)
	addStruct(t, db)
	checkCount(t, db, 4)

	updateStruct(t, db, 1)
	checkCount(t, db, 4)
	delRecord(t, db, 2)
	checkCount(t, db, 3)
}

func checkCount(t *testing.T, db *DbfTable, count int) {
	c := 0
	iter := db.NewIterator()
	for iter.Next() {
		c++
	}
	if c != count {
		t.Fatal("record count is wrong, expected", count, "found:", c)
	}
}

// delRecord delete row.
func delRecord(t *testing.T, db *DbfTable, row int) {
	db.Delete(row)
	if !db.IsDeleted(row) {
		t.Fatal("record should be deleted but it is not")
	}
}

// addRecord adds and then checks record.
func addRecord(t *testing.T, db *DbfTable) {
	row := db.AddRecord()
	//println("Row: ", row)
	db.SetFieldValueByName(row, "boolean", "t")
	db.SetFieldValueByName(row, "text", "message")
	db.SetFieldValueByName(row, "int", "44")
	db.SetFieldValueByName(row, "float", "44.123")

	arr := db.Row(row)
	if len(arr) != 4 {
		t.Fatal("record length is wrong expected 4 found:", len(arr))
	}
	if arr[0] != "t" {
		t.Fatal("record for boolean field expected 't' found:", arr[0])
	}
	if arr[1] != "message" {
		t.Fatal("expected 'message' found:", arr[1])
	}
	if arr[2] != "44" {
		t.Fatal("expected '44' found:", arr[2])
	}
	if arr[3] != "44.123" {
		t.Fatal("expected '44.123' found:", arr[3])
	}
}

// update one value in record.
func updateRecord(t *testing.T, db *DbfTable, row int) {
	nval := "123"
	db.SetFieldValue(row, 2, nval)
	v := db.FieldValue(row, 2)
	if v != nval {
		t.Fatal("update expected", nval, "found:", v)
	}
}

// update record using struct.
func updateStruct(t *testing.T, db *DbfTable, row int) {
	table := TTable{}
	db.Write(row, &TTable{Boolean: false, Text: "msgupdate", Int: 11, Float: 123.56})
	if err := db.Read(row, &table); err != nil {
		t.Fatal(err)
	}
	if table.Boolean != false {
		t.Fatal("TTable.Boolean must be false")
	}
	if table.Text != "msgupdate" {
		t.Fatal("TTable.Text expected to be 'msgupdate' found:", table.Text)
	}
	if table.Int != 11 {
		t.Fatal("TTable.Int expected to be '11' found:", table.Int)
	}
	if table.Float != 123.56 {
		t.Fatal("TTable.Float expected to be '123.56' found:", table.Float)
	}
}

// addStruct adds record using struct and checks it.
func addStruct(t *testing.T, db *DbfTable) {
	row := db.AddRecord()
	db.Write(row, TTable{Boolean: true, Text: "msg", Int: 33, Float: 44.34})

	table := TTable{}
	if err := db.Read(row, &table); err != nil {
		t.Fatal(err)
	}
	if table.Boolean != true {
		t.Fatal("TTable.Boolean must be true")
	}
	if table.Text != "msg" {
		t.Fatal("TTable.Text expected to be 'msg' found:", table.Text)
	}
	if table.Int != 33 {
		t.Fatal("TTable.Int expected to be '33' found:", table.Int)
	}
	if table.Float != 44.34 {
		t.Fatal("TTable.Float expected to be '44.34' found:", table.Float)
	}
}
