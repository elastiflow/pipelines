package errors

import (
	"errors"
	"testing"
)

func TestComponentError_Stage(t *testing.T) {

	tests := []struct {
		name    string
		error   error
		segment string
		want    string
	}{
		{
			name:    "should return the segment",
			error:   errors.New("error"),
			segment: "segment",
			want:    "segment",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := &SegmentError{
				error:   tt.error,
				segment: tt.segment,
			}

			if got := e.Stage(); got != tt.want {
				t.Errorf("Stage() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNewSegment(t *testing.T) {
	type args struct {
		segment string
		err     error
	}
	tests := []struct {
		name string
		args args
		want *SegmentError
	}{
		{
			name: "should return a new segment error",
			args: args{
				segment: "segment",
				err:     errors.New("error"),
			},
			want: &SegmentError{
				error:   errors.New("error"),
				segment: "segment",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := NewSegment(tt.args.segment, tt.args.err); got.segment != tt.want.segment {
				t.Errorf("NewSegment() = %v, want %v", got, tt.want)
			}
		})
	}
}
