package phonenumbers

import (
	"testing"
)

func TestUpdate(t *testing.T) {
	err := Update()
	if err != nil {
		t.Fatal(err)
	}
}
