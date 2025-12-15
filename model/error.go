package model

type Err string

const (
	// Err's returned by server and Clerk
	OK        Err = "OK"
	ErrNoKey   Err = "ErrNoKey"
	ErrVersion Err = "ErrVersion"

	// Err returned by Clerk only
	ErrMaybe Err = "ErrMaybe"

	// For kvraft lab
	ErrWrongLeader Err = "ErrWrongLeader"
	ErrWrongGroup  Err = "ErrWrongGroup"
)