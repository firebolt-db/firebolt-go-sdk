package types

// DescribeResult represents the structure of a describe query result
type DescribeResult struct {
	ParameterTypes map[string]string `json:"parameter_types"`
	ResultColumns  []struct {
		Name string `json:"name"`
		Type string `json:"type"`
	} `json:"result_columns"`
}
