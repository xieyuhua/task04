package queue

import "unsafe"

// String converts a byte slice to a string without memory copy (Go 1.20+).
func String(b []byte) string {
	return unsafe.String(unsafe.SliceData(b), len(b))
}

// Slice converts a string to a byte slice without memory copy (Go 1.20+).
func Slice(s string) []byte {
	return unsafe.Slice(unsafe.StringData(s), len(s))
}
