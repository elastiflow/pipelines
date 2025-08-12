package partition

import "testing"

func TestJumpHash(t *testing.T) {
	type testUser struct {
		ID   int
		Name string
	}

	tests := []struct {
		name          string
		key           interface{}
		shardCount    int
		expectedIndex uint64
	}{
		{
			name:          "A string key returns a consistent index",
			key:           "user:12345:profile",
			shardCount:    100,
			expectedIndex: 66,
		},
		{
			name:          "A different string key returns a different index",
			key:           "product:abc-def:image",
			shardCount:    100,
			expectedIndex: 52,
		},
		{
			name:          "The same key with a different shard count returns a new index",
			key:           "user:12345:profile",
			shardCount:    50,
			expectedIndex: 7,
		},
		{
			name:          "An integer key returns a consistent index",
			key:           123456789,
			shardCount:    1000,
			expectedIndex: 705,
		},
		{
			name:          "A different integer key returns a different index",
			key:           987654321,
			shardCount:    1000,
			expectedIndex: 309,
		},
		{
			name:          "A struct key returns a consistent index",
			key:           testUser{ID: 99, Name: "test-user"},
			shardCount:    100,
			expectedIndex: 69,
		},
		{
			name:          "A different struct key returns a different index",
			key:           testUser{ID: 100, Name: "another-user"},
			shardCount:    100,
			expectedIndex: 88,
		},
		{
			name:          "A shard count of one returns index zero",
			key:           "any-key",
			shardCount:    1,
			expectedIndex: 0,
		},
		{
			name:          "A shard count of zero returns index zero",
			key:           "any-key",
			shardCount:    0,
			expectedIndex: 0,
		},
		{
			name:          "A negative shard count returns index zero",
			key:           "any-key",
			shardCount:    -10,
			expectedIndex: 0,
		},
		{
			name:          "A nil key returns a consistent index",
			key:           nil,
			shardCount:    1000,
			expectedIndex: 666,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			actualIndex := JumpHash(tc.key, tc.shardCount)
			if actualIndex != tc.expectedIndex {
				t.Errorf("JumpHash() returned index %d, but expected %d", actualIndex, tc.expectedIndex)
			}
		})
	}
}

func TestModuloHash(t *testing.T) {
	type testUser struct {
		ID   int
		Name string
	}

	tests := []struct {
		name          string
		key           interface{}
		shardCount    int
		expectedIndex uint64
	}{
		{
			name:          "A string key returns a consistent index",
			key:           "user:12345:profile",
			shardCount:    100,
			expectedIndex: 76,
		},
		{
			name:          "A different string key returns a different index",
			key:           "product:abc-def:image",
			shardCount:    100,
			expectedIndex: 67,
		},
		{
			name:          "The same key with a different shard count returns a new index",
			key:           "user:12345:profile",
			shardCount:    50,
			expectedIndex: 26,
		},
		{
			name:          "An integer key returns a consistent index",
			key:           123456789,
			shardCount:    1000,
			expectedIndex: 148,
		},
		{
			name:          "A different integer key returns a different index",
			key:           987654321,
			shardCount:    1000,
			expectedIndex: 412,
		},
		{
			name:          "A struct key returns a consistent index",
			key:           testUser{ID: 99, Name: "test-user"},
			shardCount:    100,
			expectedIndex: 7,
		},
		{
			name:          "A different struct key returns a different index",
			key:           testUser{ID: 100, Name: "another-user"},
			shardCount:    100,
			expectedIndex: 81,
		},
		{
			name:          "A shard count of one returns index zero",
			key:           "any-key",
			shardCount:    1,
			expectedIndex: 0,
		},
		{
			name:          "A shard count of zero returns index zero",
			key:           "any-key",
			shardCount:    0,
			expectedIndex: 0,
		},
		{
			name:          "A negative shard count returns index zero",
			key:           "any-key",
			shardCount:    -10,
			expectedIndex: 0,
		},
		{
			name:          "A nil key returns a consistent index",
			key:           nil,
			shardCount:    1000,
			expectedIndex: 588,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			actualIndex := ModulusHash(tc.key, tc.shardCount)
			if actualIndex != tc.expectedIndex {
				t.Errorf("ModulusHash() returned index %d, but expected %d", actualIndex, tc.expectedIndex)
			}
		})
	}
}
