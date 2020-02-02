package rbac

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/amzuko/authalog"
)

type RBACAuthorizer struct {
	db *authalog.Database
}

type Role int

const (
	Reader Role = iota
	Writer
	Admin
)

type Action int

const (
	View Action = iota
	Create
	Edit
	Delete
)

type ResourceType int

const (
	Post ResourceType = iota
	Comment
)

var policy = `
checkResource(User, Action, Resource) :-
	resourceType(Resource, ResourceType),
	users(User, Role),
	allowed(Role, Action, ResourceType).

checkResourceType(User, Action, ResourceType) :-
	users(User, Role),
	allowed(Role, Action, ResourceType).

resourceType(Resource, 'Post') :-
	posts(Resource).
resourceType(Resource, 'Comment') :-
	comments(Resource).

allowed('Reader', 'View', 'Post').
allowed('Reader', 'View', 'Comment').
allowed('Reader', 'Create', 'Comment').

% Writers can do everything readers can, plus create and edit posts.
allowed('Writer', Action, ResourceType) :-
	allowed('Reader', Action, ResourceType).

allowed('Writer', 'Create', 'Post').
allowed('Writer', 'Edit', 'Post').
allowed('Writer', 'Delete', 'Post').

% Admins can do everything writers can, plus edit and delete comments.
allowed('Admin', Action, ResourceType) :-
	allowed('Writer', Action, ResourceType).
allowed('Writer', 'Edit', 'Comment').
allowed('Writer', 'Delete', 'Comment').
`

func NewRBACAuthorizer(db *sql.DB) (*RBACAuthorizer, error) {

	users, err := authalog.CreateSQLExternalRelation(authalog.SQLExternalRelationSpec{
		Table:   "users",
		Columns: []string{"id", "role"},
		Types:   []interface{}{0, Reader},
	}, db)
	if err != nil {
		return nil, err
	}
	posts, err := authalog.CreateSQLExternalRelation(authalog.SQLExternalRelationSpec{
		Table:   "posts",
		Columns: []string{"id", "author"},
		Types:   []interface{}{0, 0},
	}, db)
	if err != nil {
		return nil, err
	}
	comments, err := authalog.CreateSQLExternalRelation(authalog.SQLExternalRelationSpec{
		Table:   "comments",
		Columns: []string{"id", "author"},
		Types:   []interface{}{0, 0},
	}, db)
	if err != nil {
		return nil, err
	}

	rbac := RBACAuthorizer{
		db: authalog.NewDatabase([]authalog.ExternalRelation{users, posts, comments}),
	}

	commands, err := rbac.db.Parse(strings.NewReader(policy))
	if err != nil {
		return nil, err
	}
	for _, c := range commands {
		_, err := rbac.db.Apply(c)
		if err != nil {
			return nil, err
		}
	}
	return &rbac, nil
}

func (rbac *RBACAuthorizer) Check(user int, action Action, resource int) (bool, error) {
	// TODO: we're passing int's as strings?
	// TODO: should we be constructing a literal directly, instead of creating a string?
	q := fmt.Sprintf("checkResource(%v, '%v', '%v')?", user, action, resource)
	c, err := rbac.db.Parse(strings.NewReader(q))
	if err != nil {
		return false, err
	}
	results, err := rbac.db.Apply(c[0])
	return len(results) != 0, err
}

func (rbac *RBACAuthorizer) CheckResourceType(user int, action Action, resourceType ResourceType) (bool, error) {
	q := fmt.Sprintf("checkResourceType(%v, '%v', '%v')?", user, action, resourceType)
	c, err := rbac.db.Parse(strings.NewReader(q))
	if err != nil {
		return false, err
	}
	results, err := rbac.db.Apply(c[0])
	return len(results) != 0, err
}

func (rbac *RBACAuthorizer) Proof(user int, action Action, resource int) (string, error) {
	q := fmt.Sprintf("checkResource(%v, '%v', '%v')?", user, action, resource)
	c, err := rbac.db.Parse(strings.NewReader(q))
	if err != nil {
		return "", err
	}
	results, err := rbac.db.Apply(c[0])
	if len(results) == 0 {
		return "", fmt.Errorf("Cannot proove as check fails")
	}
	return rbac.db.ProofString(c[0].Head), nil
}
