package rbac

import "fmt"

var ErrInvalidValue = func(t string, v interface{}) error { return fmt.Errorf("Not a valid %s: %v", t, v) }
var ErrInvalidType = func(t string) error { return fmt.Errorf("Invalid type for %s", t) }

// TODO: can we fold these functions into a stringer-like utility?

func getRoleFromString(str string) *Role {
	for i := 0; i < len(_Role_index)-1; i++ {
		if str == _Role_name[_Role_index[i]:_Role_index[i+1]] {
			role := Role(i)
			return &role
		}
	}
	return nil
}

func (s *Role) Scan(input interface{}) error {
	switch input := input.(type) {
	case []uint8:
		str := string(input)
		e := getRoleFromString(str)
		if e == nil {
			return ErrInvalidValue("Role", str)
		}
		*s = *e
		return nil
	case int:
		*s = Role(input)
		return nil
	case int64:
		*s = Role(input)
		return nil
	case int32:
		*s = Role(input)
		return nil
	case string:
		e := getRoleFromString(input)
		if e == nil {
			return ErrInvalidValue("Role", input)
		}
		*s = *e
		return nil
	default:
		return ErrInvalidType("Role")
	}
}

func getActionFromString(str string) *Action {
	for i := 0; i < len(_Action_index)-1; i++ {
		if str == _Action_name[_Action_index[i]:_Action_index[i+1]] {
			a := Action(i)
			return &a
		}
	}
	return nil
}

func (s *Action) Scan(input interface{}) error {
	switch input := input.(type) {
	case []uint8:
		str := string(input)
		e := getActionFromString(str)
		if e == nil {
			return ErrInvalidValue("Action", str)
		}
		*s = *e
		return nil
	case int:
		*s = Action(input)
		return nil
	case int64:
		*s = Action(input)
		return nil
	case int32:
		*s = Action(input)
		return nil
	case string:
		e := getActionFromString(input)
		if e == nil {
			return ErrInvalidValue("Action", input)
		}
		*s = *e
		return nil
	default:
		return ErrInvalidType("Action")
	}
}

func getResourceTypeFromString(str string) *ResourceType {
	for i := 0; i < len(_ResourceType_index)-1; i++ {
		if str == _ResourceType_name[_ResourceType_index[i]:_ResourceType_index[i+1]] {
			a := ResourceType(i)
			return &a
		}
	}
	return nil
}

func (s *ResourceType) Scan(input interface{}) error {
	switch input := input.(type) {
	case []uint8:
		str := string(input)
		e := getResourceTypeFromString(str)
		if e == nil {
			return ErrInvalidValue("ResourceType", str)
		}
		*s = *e
		return nil
	case int:
		*s = ResourceType(input)
		return nil
	case int64:
		*s = ResourceType(input)
		return nil
	case int32:
		*s = ResourceType(input)
		return nil
	case string:
		e := getResourceTypeFromString(input)
		if e == nil {
			return ErrInvalidValue("ResourceType", input)
		}
		*s = *e
		return nil
	default:
		return ErrInvalidType("ResourceType")
	}
}
