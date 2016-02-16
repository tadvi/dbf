package dbf

import (
	"errors"
	"os"
	"strings"
	"time"
)

type DbfTable struct {
	// dbase file header information
	fileSignature   uint8 // Valid dBASE III PLUS table file (03h without a memo .DBT file; 83h with a memo)
	updateYear      uint8 // Date of last update; in YYMMDD format.
	updateMonth     uint8
	updateDay       uint8
	numberOfRecords uint32   // Number of records in the table.
	headerSize      uint16   // Number of bytes in the header.
	recordLength    uint16   // Number of bytes in the record.
	reservedBytes   [20]byte // Reserved bytes
	fieldDescriptor [32]byte // Field descriptor array
	fieldTerminator int8     // 0Dh stored as the field terminator.

	numberOfFields int // number of fiels/colums in dbase file

	// columns of dbase file
	fields []DbfField

	// used to map field names to index
	fieldMap map[string]int
	// list of deleted rows, helps with InsertRecord
	delRows []int
	// table structure can not be changed since it has records
	frozenStruct bool
	//
	loading bool
	// keeps the dbase table in memory as byte array
	dataStore []byte
}

type DbfField struct {
	Name       string
	Type       string
	Length     uint8
	fieldStore [32]byte
}

// Create a new dbase table from the scratch
func New() *DbfTable {
	// Create and pupulate DbaseTable struct
	dt := new(DbfTable)

	// read dbase table header information
	dt.fileSignature = 0x03
	dt.updateYear = byte(time.Now().Year() % 100)
	dt.updateMonth = byte(time.Now().Month())
	dt.updateDay = byte(time.Now().YearDay())
	dt.numberOfRecords = 0
	dt.headerSize = 32
	dt.recordLength = 0

	// create fieldMap to taranslate field name to index
	dt.fieldMap = make(map[string]int)

	// Number of fields in dbase table
	dt.numberOfFields = int((dt.headerSize - 1 - 32) / 32)
	s := make([]byte, dt.headerSize)

	// set DbfTable dataStore slice that will store the complete file in memory
	dt.dataStore = s

	dt.dataStore[0] = dt.fileSignature
	dt.dataStore[1] = dt.updateYear
	dt.dataStore[2] = dt.updateMonth
	dt.dataStore[3] = dt.updateDay

	// no MDX file (index upon demand)
	dt.dataStore[28] = 0x00
	dt.dataStore[28] = 0xf0 // default to UTF-8 encoding, use 0x57 for ANSI.
	return dt
}

func (df *DbfField) SetFieldName(fieldName string) {
	df.Name = fieldName
}

// LoadFile load dBase III+ from file.
func LoadFile(fileName string) (table *DbfTable, err error) {
	s, err := readFile(fileName)
	if err != nil {
		return nil, err
	}
	// Create and pupulate DbaseTable struct
	dt := new(DbfTable)
	dt.loading = true
	// set DbfTable dataStore slice that will store the complete file in memory
	dt.dataStore = s

	// read dbase table header information
	dt.fileSignature = s[0]
	dt.updateYear = s[1]
	dt.updateMonth = s[2]
	dt.updateDay = s[3]
	dt.numberOfRecords = uint32(s[4]) | (uint32(s[5]) << 8) | (uint32(s[6]) << 16) | (uint32(s[7]) << 24)
	dt.headerSize = uint16(s[8]) | (uint16(s[9]) << 8)
	dt.recordLength = uint16(s[10]) | (uint16(s[11]) << 8)

	// create fieldMap to taranslate field name to index
	dt.fieldMap = make(map[string]int)

	// Number of fields in dbase table
	dt.numberOfFields = int((dt.headerSize - 1 - 32) / 32)

	// populate dbf fields
	for i := 0; i < int(dt.numberOfFields); i++ {
		offset := (i * 32) + 32

		fieldName := strings.Trim(string(s[offset:offset+10]), string([]byte{0}))
		dt.fieldMap[fieldName] = i

		var err error
		switch s[offset+11] {
		case 'C':
			err = dt.AddTextField(fieldName, s[offset+16])
		case 'N':
			err = dt.AddNumberField(fieldName, s[offset+16], s[offset+17])
		case 'L':
			err = dt.AddBoolField(fieldName)
		case 'D':
			err = dt.AddDateField(fieldName)
		}

		if err != nil {
			return nil, err
		}
	}

	// memorize deleted rows
	sz := int(dt.numberOfRecords)
	for i := 0; i < sz; i++ {
		if dt.IsDeleted(i) {
			dt.delRows = append(dt.delRows, i)
		}
	}

	dt.frozenStruct = true
	return dt, nil
}

// SaveFile dbf file.
func (dt *DbfTable) SaveFile(filename string) error {
	// don't forget to add dbase end of file marker which is 1Ah
	dt.dataStore = appendSlice(dt.dataStore, []byte{0x1A})
	f, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer f.Close()

	_, err = f.Write(dt.dataStore)
	if err != nil {
		return err
	}
	return nil
}

// Sets field value by name.
func (dt *DbfTable) SetFieldValueByName(row int, fieldName string, value string) {
	fieldName = strings.ToUpper(fieldName)
	fieldIndex, ok := dt.fieldMap[fieldName]
	if !ok {
		panic("Field name '" + fieldName + "' does not exist")
	}
	// set field value and return
	dt.SetFieldValue(row, fieldIndex, value)
}

func (dt *DbfTable) getRowOffset(row int) int {
	// locate the offset of the field in DbfTable dataStore
	offset := int(dt.headerSize)
	recordLength := int(dt.recordLength)
	return offset + (row * recordLength)
}

func (dt *DbfTable) findSpot() int {
	// use prior deleted row
	row := -1
	if len(dt.delRows) > 0 {
		last := len(dt.delRows) - 1
		row, dt.delRows = dt.delRows[last], dt.delRows[:last]
	}
	return row
}

// Delete row by setting marker.
func (dt *DbfTable) Delete(row int) {
	dt.dataStore[dt.getRowOffset(row)] = 0x2A // set deleted record marker
	dt.delRows = append(dt.delRows, row)
}

// IsDeleted row.
func (dt *DbfTable) IsDeleted(row int) bool {
	return dt.dataStore[dt.getRowOffset(row)] == 0x2A
}

// Sets field value by index.
func (dt *DbfTable) SetFieldValue(row int, fieldIndex int, value string) {
	dt.frozenStruct = true // table structure can not be changed from this point

	b := []byte(value)
	fieldLength := int(dt.fields[fieldIndex].Length)

	// locate the offset of the field in DbfTable dataStore
	offset := dt.getRowOffset(row)
	recordOffset := 1

	//fmt.Println("total fields: ", dt.fields)
	for i := 0; i < len(dt.fields); i++ {
		if i == fieldIndex {
			break
		} else {
			recordOffset += int(dt.fields[i].Length)
		}
	}

	// first fill the field with space values
	for i := 0; i < fieldLength; i++ {
		dt.dataStore[offset+recordOffset+i] = 0x20
	}

	// write new value
	// TODO: this should use copy() or other fast way to move data
	switch dt.fields[fieldIndex].Type {
	case "C", "L", "D":
		for i := 0; i < len(b) && i < fieldLength; i++ {
			dt.dataStore[offset+recordOffset+i] = b[i]
		}
	case "N":
		for i := 0; i < fieldLength; i++ {
			if i < len(b) {
				dt.dataStore[offset+recordOffset+(fieldLength-i-1)] = b[(len(b)-1)-i]
			} else {
				break
			}
		}
	}
}

func (dt *DbfTable) FieldValue(row int, fieldIndex int) string {
	offset := int(dt.headerSize)
	recordLength := int(dt.recordLength)

	offset = offset + (row * recordLength)
	recordOffset := 1

	for i := 0; i < len(dt.fields); i++ {
		if i == fieldIndex {
			break
		} else {
			recordOffset += int(dt.fields[i].Length)
		}
	}

	temp := dt.dataStore[(offset + recordOffset):((offset + recordOffset) + int(dt.fields[fieldIndex].Length))]
	for i := 0; i < len(temp); i++ {
		if temp[i] == 0x00 {
			temp = temp[0:i]
			break
		}
	}
	s := string(temp)
	return strings.TrimSpace(s)
}

// FieldValueByName retuns the value of a field given row number and fieldName provided.
func (dt *DbfTable) FieldValueByName(row int, fieldName string) string {
	fieldName = strings.ToUpper(fieldName)
	fieldIndex, ok := dt.fieldMap[fieldName]
	if !ok {
		panic("Field name '" + fieldName + "' does not exist")
	}
	return dt.FieldValue(row, fieldIndex)
}

// InsertRecord tries to reuse deleted records, and only then add new record to the
// end of file if no delete slots exist.
// If you are looping over rows it is better to use AddRecord.
func (dt *DbfTable) InsertRecord() int {
	if row := dt.findSpot(); row > -1 {
		// undelete selected row
		dt.dataStore[dt.getRowOffset(row)] = 0x20
		return row
	}
	return dt.AddRecord()
}

// AddRecord always adds new rows to the end of file.
func (dt *DbfTable) AddRecord() int {
	newRecord := make([]byte, dt.recordLength)
	dt.dataStore = appendSlice(dt.dataStore, newRecord)

	// since row numbers are "0" based first we set newRecordNumber
	// and then increment number of records in dbase table
	newRecordNumber := int(dt.numberOfRecords)

	dt.numberOfRecords++
	s := uint32ToBytes(dt.numberOfRecords)
	dt.dataStore[4] = s[0]
	dt.dataStore[5] = s[1]
	dt.dataStore[6] = s[2]
	dt.dataStore[7] = s[3]
	return newRecordNumber
}

// AddTextField max size 254 bytes.
func (dt *DbfTable) AddTextField(fieldName string, length uint8) error {
	return dt.addField(fieldName, 'C', length, 0)
}

// AddNumberField can be used to add int or float number fields.
func (dt *DbfTable) AddNumberField(fieldName string, length uint8, prec uint8) error {
	return dt.addField(fieldName, 'N', length, prec)
}

// AddIntField add int.
func (dt *DbfTable) AddIntField(fieldName string) error {
	return dt.addField(fieldName, 'N', 17, 0)
}

// AddFloatField add float.
func (dt *DbfTable) AddFloatField(fieldName string) error {
	return dt.addField(fieldName, 'N', 17, 8)
}

// Boolean field stores 't' or 'f' in the cell.
func (dt *DbfTable) AddBoolField(fieldName string) error {
	return dt.addField(fieldName, 'L', 1, 0)
}

func (dt *DbfTable) AddDateField(fieldName string) error {
	return dt.addField(fieldName, 'D', 8, 0)
}

// NumRecords return number of rows in dbase table.
func (dt *DbfTable) NumRecords() int {
	return int(dt.numberOfRecords)
}

// Fields return slice of DbfField
func (dt *DbfTable) Fields() []DbfField {
	return dt.fields
}

func (dt *DbfTable) addField(fieldName string, fieldType byte, length, prec uint8) error {
	if dt.frozenStruct {
		return errors.New("once you start entering data into the dBase table altering dBase table schema is not allowed")
	}

	s := dt.getNormalizedFieldName(fieldName)
	if dt.isFieldExist(s) {
		return errors.New("Field with name '" + s + "' already exist!")
	}

	df := new(DbfField)
	df.Name = s
	df.Type = string(fieldType)
	df.Length = length

	slice := dt.convertToByteSlice(s, 10)
	// Field name in ASCII (max 10 chracters)
	for i := 0; i < len(slice); i++ {
		df.fieldStore[i] = slice[i]
	}

	// Field names are terminated by 00h
	df.fieldStore[10] = 0x00

	// Set field's data type
	// C (Character) 	All OEM code page characters.
	// D (Date) 		Numbers and a character to separate month, day, and year (stored internally as 8 digits in YYYYMMDD format).
	// N (Numeric) 		- . 0 1 2 3 4 5 6 7 8 9
	// L (Logical) 		? Y y N n T t F f (? when not initialized).
	df.fieldStore[11] = fieldType

	// length and precision of the field
	df.fieldStore[16] = length
	df.fieldStore[17] = prec
	dt.fields = append(dt.fields, *df)

	if !dt.loading {
		dt.updateHeader()
	}
	return nil
}

// updateHeader updates the dbase file header after a field added
func (dt *DbfTable) updateHeader() {
	// first create a slice from initial 32 bytes of datastore as the foundation of the new slice
	// later we will set this slice to dt.dataStore to create the new header slice
	slice := dt.dataStore[0:32]

	// set dbase file signature
	slice[0] = 0x03
	var recordLength uint16 = 0

	for i := range dt.Fields() {
		recordLength += uint16(dt.Fields()[i].Length)
		slice = appendSlice(slice, dt.Fields()[i].fieldStore[:])

		// don't forget to update fieldMap. We need it to find the index of a field name
		dt.fieldMap[dt.Fields()[i].Name] = i
	}

	// end of file header terminator (0Dh)
	slice = appendSlice(slice, []byte{0x0D})

	// now reset dt.dataStore slice with the updated one
	dt.dataStore = slice

	// update the number of bytes in dbase file header
	dt.headerSize = uint16(len(slice))
	s := uint32ToBytes(uint32(dt.headerSize))
	dt.dataStore[8] = s[0]
	dt.dataStore[9] = s[1]

	dt.recordLength = recordLength + 1 // dont forget to add "1" for deletion marker which is 20h

	// update the lenght of each record
	s = uint32ToBytes(uint32(dt.recordLength))
	dt.dataStore[10] = s[0]
	dt.dataStore[11] = s[1]
}

// Row reads record at index.
func (dt *DbfTable) Row(row int) []string {
	s := make([]string, len(dt.Fields()))
	for i := 0; i < len(dt.Fields()); i++ {
		s[i] = dt.FieldValue(row, i)
	}
	return s
}

func (dt *DbfTable) isFieldExist(name string) bool {
	for i := 0; i < len(dt.fields); i++ {
		if dt.fields[i].Name == name {
			return true
		}
	}
	return false
}

// convertToByteSlice converts value to byte slice.
func (dt *DbfTable) convertToByteSlice(value string, numberOfBytes int) []byte {
	b := []byte(value)
	if len(b) <= numberOfBytes {
		return b
	}
	return b[0:numberOfBytes]
}

func (dt *DbfTable) getNormalizedFieldName(name string) string {
	b := []byte(name)
	if len(b) > 10 {
		b = b[0:10]
	}
	return strings.ToUpper(string(b))
}
