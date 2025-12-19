package api

type Err string

const (
	// Err's returned by server and Clerk
	OK        Err = "OK"
	ErrNoKey   Err = "ErrNoKey"
	ErrVersion Err = "ErrVersion"

	// Err returned by Clerk only
	ErrMaybe Err = "ErrMaybe"
)