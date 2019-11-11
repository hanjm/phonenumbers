package phonenumbers

import (
	"testing"
)

func TestUpdate(t *testing.T) {
	err := update("/tmp/")
	if err != nil {
		t.Fatal(err)
	}
}
