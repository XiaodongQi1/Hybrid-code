package triecodec

// ValidateNode validates one canonical node and its inline descendants without
// fetching hashed siblings. This bounds nesting/path work for untrusted proofs.
func ValidateNode(raw []byte) error {
	c := DefaultConfig()
	c.MaxPathNibbles = 64
	_, e := parseNode(raw, c)
	return e
}
