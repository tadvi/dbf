package dbf

import (
	"fmt"
	"reflect"
	"strconv"
)

type Iterator struct {
	dt     *DbfTable
	index  int
	last   int
	offset int
}

func (dt *DbfTable) NewIterator() *Iterator {
	return &Iterator{dt: dt, index: -1, offset: -1, last: dt.NumRecords()}
}

func (it *Iterator) Index() int {
	return it.index
}

// Next iterates over records in the table.
func (it *Iterator) Next() bool {
	for it.index++; it.index < it.last; it.index++ {
		if !it.dt.IsDeleted(it.index) {
			return true
		}
	}
	return false // it.index < it.last
}

// Read data into struct.
func (it *Iterator) Read(spec interface{}) error {
	return it.dt.Read(it.index, spec)
}

// Write record where iterator points to.
func (it *Iterator) Write(spec interface{}) int {
	return it.dt.Write(it.index, spec)
}

// Delete row under iterator. This is possible because rows are marked as deleted
// but are not physically deleted.
func (it *Iterator) Delete() {
	it.dt.Delete(it.index)
}

// Row data as raw slice.
func (it *Iterator) Row() []string {
	return it.dt.Row(it.index)
}

// Create schema based on the spec struct.
func (dt *DbfTable) Create(spec interface{}) error {
	s := reflect.ValueOf(spec)
	if s.Kind() == reflect.Ptr {
		s = s.Elem()
	}
	if s.Kind() != reflect.Struct {
		panic("dbf: spec parameter must be a struct")
	}

	var err error
	typeOfSpec := s.Type()
	for i := 0; i < s.NumField(); i++ {
		var sz uint8 = 50 // text fields default to 50 unless specified
		f := s.Field(i)
		if typeOfSpec.Field(i).PkgPath != "" || typeOfSpec.Field(i).Anonymous {
			continue // ignore unexported or embedded fields
		}
		fieldName := typeOfSpec.Field(i).Name
		alt := typeOfSpec.Field(i).Tag.Get("dbf")
		// ignore '-' tags
		if alt == "-" {
			continue
		}
		if alt != "" {
			n, err := strconv.ParseUint(alt, 0, 8)
			if err != nil {
				panic("dbf: invalid struct tag " + alt)
			}
			sz = uint8(n)
		}

		switch f.Kind() {
		default:
			panic("dbf: unsupported type for database table schema, use dash to omit")

		case reflect.String:
			err = dt.AddTextField(fieldName, sz)

		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
			reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			err = dt.AddIntField(fieldName)

		case reflect.Bool:
			err = dt.AddBoolField(fieldName)

		case reflect.Float32, reflect.Float64:
			err = dt.AddFloatField(fieldName)
		}

		if err != nil {
			return err
		}
	}
	return nil
}

// Append record to table.
func (dt *DbfTable) Append(spec interface{}) int {
	return dt.Write(dt.AddRecord(), spec)
}

// Write data into DbfTable from the spec.
func (dt *DbfTable) Write(row int, spec interface{}) int {
	s := reflect.ValueOf(spec)
	if s.Kind() == reflect.Ptr {
		s = s.Elem()
	}
	if s.Kind() != reflect.Struct {
		panic("dbf: spec parameter must be a struct")
	}

	typeOfSpec := s.Type()
	for i := 0; i < s.NumField(); i++ {
		f := s.Field(i)
		if typeOfSpec.Field(i).PkgPath != "" || typeOfSpec.Field(i).Anonymous {
			continue // ignore unexported or embedded fields
		}

		alt := typeOfSpec.Field(i).Tag.Get("dbf")
		// ignore '-' tags
		if alt == "-" {
			continue
		}

		val := ""
		switch f.Kind() {
		default:
			panic("dbf: unsupported type for database table schema, use dash to omit")
		case reflect.String:
			val = f.String()
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
			reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			val = fmt.Sprintf("%d", f.Int())
		case reflect.Bool:
			val = "f"
			if f.Bool() {
				val = "t"
			}
		case reflect.Float32, reflect.Float64:
			val = fmt.Sprintf("%f", f.Float())
		}

		//fmt.Println(typeOfSpec.Field(i).Name)
		dt.SetFieldValueByName(row, typeOfSpec.Field(i).Name, val)
	}
	return row
}

// Read data into the spec from DbfTable.
func (dt *DbfTable) Read(row int, spec interface{}) error {
	v := reflect.ValueOf(spec)
	if v.Kind() != reflect.Ptr {
		panic("dbf: must be a pointer")
	}
	s := v.Elem()
	if s.Kind() != reflect.Struct {
		panic("dbf: spec parameter must be a struct")
	}

	typeOfSpec := s.Type()
	for i := 0; i < s.NumField(); i++ {
		f := s.Field(i)
		if f.CanSet() {
			var fieldName string
			alt := typeOfSpec.Field(i).Tag.Get("dbf")

			// ignore '-' tags
			if alt == "-" {
				continue
			}
			fieldName = typeOfSpec.Field(i).Name
			value := dt.FieldValueByName(row, fieldName)

			switch f.Kind() {
			default:
				panic("dbf: unsupported type for database table schema, use dash to omit")

			case reflect.String:
				f.SetString(value)

			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
				reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
				intValue, err := strconv.ParseInt(value, 0, f.Type().Bits())
				if err != nil {
					return fmt.Errorf("fail to parse field '%s' type: %s value: %s",
						fieldName, f.Type().String(), value)
				}
				f.SetInt(intValue)

			case reflect.Bool:
				if value == "T" || value == "t" || value == "Y" || value == "y" {
					f.SetBool(true)
				} else {
					f.SetBool(false)
				}

			case reflect.Float32, reflect.Float64:
				floatValue, err := strconv.ParseFloat(value, f.Type().Bits())
				if err != nil {
					return fmt.Errorf("fail to parse field '%s' type: %s value: %s",
						fieldName, f.Type().String(), value)
				}
				f.SetFloat(floatValue)
			}
		}
	}
	return nil
}
