package datastreams

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
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
		{
			name:    "should create a SINK error",
			code:    SINK,
			segment: "segment4",
			msg:     "sink error",
			want:    "datastream SINK error (code: 3 segment: segment4, message: sink error)",
		},
		{
			name:    "should create an EXPAND error",
			code:    EXPAND,
			segment: "segment5",
			msg:     "expand error",
			want:    "datastream EXPAND error (code: 4 segment: segment5, message: expand error)",
		},
		{
			name:    "should create an EXPAND error",
			code:    KEY_BY,
			segment: "segment5",
			msg:     "key by error",
			want:    "datastream KEY_BY error (code: 5 segment: segment5, message: key by error)",
		},
		{
			name:    "should create an EXPAND error",
			code:    WINDOW,
			segment: "segment5",
			msg:     "window error",
			want:    "datastream WINDOW error (code: 6 segment: segment5, message: window error)",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := newError(tt.code, tt.segment, tt.msg)
			assert.ErrorContains(t, err, tt.want, "error message should match")
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
			assert.ErrorContains(t, err, tt.want, "error message should match")

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
			assert.ErrorContains(t, err, tt.want, "error message should match")
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
			assert.ErrorContains(t, err, tt.want, "error message should match")
		})
	}
}

func TestNewSinkError(t *testing.T) {
	tests := []struct {
		name    string
		segment string
		err     error
		want    string
	}{
		{
			name:    "should create a SINK error",
			segment: "segment4",
			err:     errors.New("sink error"),
			want:    "datastream SINK error (code: 3 segment: segment4, message: sink error)",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := newSinkError(tt.segment, tt.err)
			assert.ErrorContains(t, err, tt.want, "error message should match")

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
			got := IsRunError(tt.err)
			assert.Equal(t, got, tt.want)
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
			got := IsFilterError(tt.err)
			assert.Equalf(t, got, tt.want, "IsFilterError() = %v, want %v", got, tt.want)
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
			got := IsMapError(tt.err)
			assert.Equalf(t, got, tt.want, "IsMapError() = %v, want %v", got, tt.want)
		})
	}
}

func TestIsSinkError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{
			name: "should return true for SINK error",
			err:  newSinkError("segment", errors.New("error")),
			want: true,
		},
		{
			name: "should return false for non-SINK error",
			err:  errors.New("some other error"),
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := IsSinkError(tt.err)
			assert.Equalf(t, got, tt.want, "IsSinkError() = %v, want %v", got, tt.want)
		})
	}
}

func TestIsExpandError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{
			name: "should return true for EXPAND error",
			err:  newExpandError("segment", errors.New("error")),
			want: true,
		},
		{
			name: "should return false for non-EXPAND error",
			err:  errors.New("some other error"),
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := IsExpandError(tt.err)
			assert.Equalf(t, got, tt.want, "IsExpandError() = %v, want %v", got, tt.want)
		})
	}
}
