
#### dbf dBase III+ library for Go

Package for working with dBase III plus database files.
Default encoding is UTF-8.

1. Package provides both reflection-via-struct interface and direct Row()/FieldValueByName()/AddxxxField() interface.
2. Once table is created and rows added to it, table structure can not be modified.
3. Working with reflection-via-struct interface is easier and produces less verbose code.
4. Use Iterator to iterate over table since it skips deleted rows.

Typical usage
db := dbf.New() or dbf.LoadFile(filename)

then use db.NewIterator() and iterate or db.Append()

do not forget db.SaveFile(filename) if you want changes saved.

## TODO

File is loaded and kept in-memory. Not a good design choice if file is huge.
This should be changed to use buffers and keep some of the data on-disk in the future.
Current API structure should allow redesign.

## Where to start

Look into cmd directory for examples of use and basic tools to load and export into CSV files.

## License

Copyright (C) Tad Vizbaras. Released under MIT license.

