// Code generated by "stringer --type ResourceType"; DO NOT EDIT.

package constants

import "strconv"

const _ResourceType_name = "PostComment"

var _ResourceType_index = [...]uint8{0, 4, 11}

func (i ResourceType) String() string {
	if i < 0 || i >= ResourceType(len(_ResourceType_index)-1) {
		return "ResourceType(" + strconv.FormatInt(int64(i), 10) + ")"
	}
	return _ResourceType_name[_ResourceType_index[i]:_ResourceType_index[i+1]]
}
