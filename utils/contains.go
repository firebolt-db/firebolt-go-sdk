package utils

// ContainsString checks whether a string is present in a slice
func ContainsString(s []string, e string) bool {
	for _, a := range s {
		if a == e {
			return true
		}
	}
	return false
}
