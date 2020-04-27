package gbac

import (
	"database/sql"
	"os"
	"testing"

	. "github.com/amzuko/authalog/examples/constants"
	_ "github.com/mattn/go-sqlite3"
)

func setupDB(db *sql.DB) error {
	_, err := db.Exec(`
	CREATE TABLE users (
		id integer,
		name text,
	);
	INSERT INTO users (id, name) VALUES
	(1, 'Loki'),
	(2, 'Quincy'),
	(3, 'Flo'),
	(4, 'Nibs'),
	(5, 'Hercules'),
	(6, 'Shrek');

	CREATE TABLE groups (
		id integer,
		name text,
	);
	INSERT INTO groups (id, name) VALUES
	(8, 'Arizona Dogs'),
	(9, 'California Dogs'),
	(10, 'Family Dogs'),
	(11, 'Dogs'),
	(12, 'Good Dogs');

	CREATE TABLE members(
		userID REFERENCES users(id),
		groupID REFERENCES groups(id),
	);
	INSERT INTO members (userID, groupID) VALUES
	(1, 9),
	(1, 12),
	(2, 8),
	(3, 10),
	(3, 12),
	(4, 8),
	(5, 11);


	CREATE TABLE subGroups(
		groupID REFERENCES groups(id),
		subGroupID REFERENCES groups(id),
	);
	INSERT INTO subgroups (groupID subGroupID) VALUES
	(10, 8),
	(10, 9),
	(11, 10),
	(11, 12);


	CREATE TABLE resources(
		id integer,
		name text,
		owner REFERENCES groups(id),
	);

	`)
	return err
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
