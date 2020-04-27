package gbac

import (
	"database/sql"
	"os"
	"testing"

	"github.com/amzuko/authalog"
)

type GBACAuthorizer struct {
	db *authalog.Database
}

var policy = `
checkResource(User, Action, Resource) :-
	owner()

member(U, G)
`

func NewGBACAuthorizer(db *sql.DB) (*GBACAuthorizer, error) {

}

func TestGBAC(t *testing.T) {
	os.Remove("test.db")
	db, err := sql.Open("sqlite3", "./test.db")
	if err != nil {
		t.Error(err)
	}
	err = setupDB(db)

	if err != nil {
		t.Error(err)
	}

	rbac, err := NewGBACAuthorizer(db)
	if err != nil {
		t.Error(err)
	}

}
