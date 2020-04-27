package constants

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
