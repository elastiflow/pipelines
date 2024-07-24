package module_template

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewEnv(t *testing.T) {
	testCases := []struct {
		name      string
		someThing string
		want      string
	}{
		{
			name:      "default value",
			someThing: "stub_value",
			want:      "stub_value",
		},
	}

	for _, tc := range testCases {
		tc := tc // capture range variable: https://blog.golang.org/subtests
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			a := tc.someThing

			assert.Equal(t, tc.want, a, "Should be equal")
		})
	}
}

// "Fail if coverage less than" will work only with Go 1.21+
// Details: https://github.com/golang/go/issues/59590#issuecomment-1609358692
// func TestMain(m *testing.M) {
// 	rc := m.Run()

// 	coverageThreshold, ok := os.LookupEnv("GO_TEST_COVERAGE_THRESHOLD")
// 	if !ok {
// 		coverageThreshold = "95"
// 	}

// 	coverageLte, err := strconv.ParseFloat(coverageThreshold, 64)
// 	if err != nil {
// 		panic(fmt.Sprintf("Can't convert go test coverage threshold to float64, err:\n%s", err))
// 	}
// 	coverageLte = coverageLte / 100

// 	if rc == 0 && testing.CoverMode() != "" {
// 		c := testing.Coverage()
// 		if c <= coverageLte {
// 			fmt.Printf("Tests passed but coverage failed at %.2f%%\n", c*100)
// 			fmt.Printf("Want coverage gte %.2f%%\n", coverageLte*100)
// 			rc = 1
// 		}
// 	}
// 	os.Exit(rc)
// }
