package datastreams

import (
	"fmt"
	"strings"
)

// ErrorCode represents a generic enrich ErrorCode
type ErrorCode int

const (
	RUN ErrorCode = iota
	FILTER
	MAP
	SINK
	EXPAND
	KEY_BY
	WINDOW
)

// String converts ErrorCode enum into a string value
func (w ErrorCode) String() string {
	return [...]string{
		"RUN",
		"FILTER",
		"MAP",
		"SINK",
		"EXPAND",
		"KEY_BY",
		"WINDOW",
	}[w]
}

// Message converts ErrorCode enum into a human-readable message
func (w ErrorCode) Message(msg string, segment string) string {
	return fmt.Sprintf(
		"datastream %s error (code: %d segment: %s, message: %s)", w.String(), w, segment, msg,
	)
}

// Error defines a custom error type
type Error struct {
	Code    ErrorCode
	Segment string
	Message string
}

// Error implements the Error interface
func (e *Error) Error() string {
	return e.Message
}

func newError(code ErrorCode, segment string, msg string) error {
	return &Error{
		Code:    code,
		Message: code.Message(msg, segment),
	}
}

func newRunError(segment string, err error) error {
	return newError(RUN, segment, err.Error())
}

func newFilterError(segment string, err error) error {
	return newError(FILTER, segment, err.Error())
}

func newMapError(segment string, err error) error {
	return newError(MAP, segment, err.Error())
}

func newSinkError(segment string, err error) error {
	return newError(SINK, segment, err.Error())
}

func newExpandError(segment string, err error) error {
	return newError(EXPAND, segment, err.Error())
}

func isError(err error, code ErrorCode) bool {
	return strings.Contains(
		err.Error(),
		fmt.Sprintf("datastream %s error (code: %d", code.String(), code),
	)
}

// IsRunError checks if the given error is a RUN error.
// It returns true if the error is a RUN error, otherwise false.
func IsRunError(err error) bool {
	return isError(err, RUN)
}

// IsFilterError checks if the given error is a FILTER error.
// It returns true if the error is a FILTER error, otherwise false.
func IsFilterError(err error) bool {
	return isError(err, FILTER)
}

// IsMapError checks if the given error is a MAP error.
// It returns true if the error is a MAP error, otherwise false.
func IsMapError(err error) bool {
	return isError(err, MAP)
}

// IsExpandError checks if the given error is an EXPAND error.
// It returns true if the error is an EXPAND error, otherwise false.
func IsExpandError(err error) bool {
	return isError(err, EXPAND)
}

func IsSinkError(err error) bool {
	return isError(err, SINK)
}
