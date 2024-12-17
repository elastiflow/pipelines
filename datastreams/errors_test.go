package datastreams

import (
	"errors"
	"testing"
)

func TestNewError(t *testing.T) {
	tests := []struct {
		name    string
		code    ErrorCode
		segment string
		msg     string
		want    string
	}{
		{
			name:    "should create a RUN error",
			code:    RUN,
			segment: "segment1",
			msg:     "run error",
			want:    "datastream RUN error (code: 0 segment: segment1, message: run error)",
		},
		{
			name:    "should create a FILTER error",
			code:    FILTER,
			segment: "segment2",
			msg:     "filter error",
			want:    "datastream FILTER error (code: 1 segment: segment2, message: filter error)",
		},
		{
			name:    "should create a MAP error",
			code:    MAP,
			segment: "segment3",
			msg:     "map error",
			want:    "datastream MAP error (code: 2 segment: segment3, message: map error)",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := newError(tt.code, tt.segment, tt.msg)
			if err.Error() != tt.want {
				t.Errorf("newError() = %v, want %v", err.Error(), tt.want)
			}
		})
	}
}

func TestNewRunError(t *testing.T) {
	tests := []struct {
		name    string
		segment string
		err     error
		want    string
	}{
		{
			name:    "should create a RUN error",
			segment: "segment1",
			err:     errors.New("run error"),
			want:    "datastream RUN error (code: 0 segment: segment1, message: run error)",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := newRunError(tt.segment, tt.err)
			if err.Error() != tt.want {
				t.Errorf("newRunError() = %v, want %v", err.Error(), tt.want)
			}
		})
	}
}

func TestNewFilterError(t *testing.T) {
	tests := []struct {
		name    string
		segment string
		err     error
		want    string
	}{
		{
			name:    "should create a FILTER error",
			segment: "segment2",
			err:     errors.New("filter error"),
			want:    "datastream FILTER error (code: 1 segment: segment2, message: filter error)",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := newFilterError(tt.segment, tt.err)
			if err.Error() != tt.want {
				t.Errorf("newFilterError() = %v, want %v", err.Error(), tt.want)
			}
		})
	}
}

func TestNewMapError(t *testing.T) {
	tests := []struct {
		name    string
		segment string
		err     error
		want    string
	}{
		{
			name:    "should create a MAP error",
			segment: "segment3",
			err:     errors.New("map error"),
			want:    "datastream MAP error (code: 2 segment: segment3, message: map error)",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := newMapError(tt.segment, tt.err)
			if err.Error() != tt.want {
				t.Errorf("newMapError() = %v, want %v", err.Error(), tt.want)
			}
		})
	}
}
func TestIsRunError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{
			name: "should return true for RUN error",
			err:  newRunError("segment", errors.New("error")),
			want: true,
		},
		{
			name: "should return false for non-RUN error",
			err:  errors.New("some other error"),
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsRunError(tt.err); got != tt.want {
				t.Errorf("IsRunError() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestIsFilterError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{
			name: "should return true for FILTER error",
			err:  newFilterError("segment", errors.New("error")),
			want: true,
		},
		{
			name: "should return false for non-FILTER error",
			err:  errors.New("some other error"),
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsFilterError(tt.err); got != tt.want {
				t.Errorf("IsFilterError() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestIsMapError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{
			name: "should return true for MAP error",
			err:  newMapError("segment", errors.New("error")),
			want: true,
		},
		{
			name: "should return false for non-MAP error",
			err:  errors.New("some other error"),
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsMapError(tt.err); got != tt.want {
				t.Errorf("IsMapError() = %v, want %v", got, tt.want)
			}
		})
	}
}
