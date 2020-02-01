package rbac

import (
	"database/sql"
	"fmt"
	"os"
	"testing"

	"github.com/amzuko/authalog"

	_ "github.com/mattn/go-sqlite3"
)

func setupDB(db *sql.DB) error {
	_, err := db.Exec(`
	CREATE TABLE users (
		id integer,
		name text,
		role integer
	);
	INSERT INTO users (id, name, role) VALUES
	(1, 'Loki', 2),
	(2, 'Quincy', 0),
	(3, 'Flo', 1),
	(4, 'Nibs', 1),
	(5, 'Arlo', 1);

	CREATE TABLE posts (
		id integer,
		author integer,
		FOREIGN KEY(author) REFERENCES users(id)
	);
	INSERT INTO posts (id, author) VALUES
	(11, 3),
	(12, 3),
	(13, 4),
	(14, 5);

	CREATE TABLE comments (
		id integer,
		post integer,
		author integer,
		FOREIGN KEY(author) REFERENCES users(id),
		FOREIGN KEY(post) REFERENCES posts(id)
	);
	INSERT INTO comments (id, post, author) VALUES
	(21, 11, 2),
	(22, 11, 3),
	(23, 11, 2),
	(24, 11, 5),
	(25, 13, 1);
`)
	if err != nil {
		return err
	}
	return nil
}

func truthy(t *testing.T) func(bool, error) {
	return func(b bool, err error) {
		if err != nil {
			t.Error(err)
		}
		if !b {
			t.Error("Expected true, got false")
		}
	}
}

func falsey(t *testing.T) func(bool, error) {
	return func(b bool, err error) {
		if err != nil {
			t.Error(err)
		}
		if b {
			t.Error("Expected false, got true")
		}
	}
}

func TestRBAC(t *testing.T) {
	os.Remove("test.db")
	db, err := sql.Open("sqlite3", "./test.db")
	if err != nil {
		t.Error(err)
	}
	err = setupDB(db)

	if err != nil {
		t.Error(err)
	}

	rbac, err := NewRBACAuthorizer(db)
	if err != nil {
		t.Error(err)
	}

	c := rbac.db.ParseCommandOrPanic("users('2', Y)?")
	r, err := authalog.Apply(c, rbac.db)
	if err != nil {
		t.Error(err)
	}
	fmt.Println(rbac.db.ToString(r))

	assertTrue := truthy(t)
	assertFalse := falsey(t)
	// Is Quincy allowed to create posts?
	rbac.CheckResourceType(2, Create, Post)
	rbac.CheckResourceType(2, Create, Comment)
	assertFalse(rbac.CheckResourceType(2, Create, Post))

	// Is Quincy allowed to create posts?
	assertTrue(rbac.CheckResourceType(2, Create, Comment))

}
