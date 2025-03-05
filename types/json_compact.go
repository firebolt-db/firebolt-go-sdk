package types

type Column struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

type Location struct {
	FailingLine int `json:"failingLine"`
	StartOffset int `json:"startOffset"`
	EndOffset   int `json:"endOffset"`
}

type ErrorDetails struct {
	Code        string   `json:"code"`
	Name        string   `json:"name"`
	Severity    string   `json:"severity"`
	Source      string   `json:"source"`
	Description string   `json:"description"`
	Resolution  string   `json:"resolution"`
	HelpLink    string   `json:"helpLink"`
	Location    Location `json:"location"`
}

type QueryResponse struct {
	Query      interface{}     `json:"query"`
	Meta       []Column        `json:"meta"`
	Data       [][]interface{} `json:"data"`
	Rows       int             `json:"rows"`
	Errors     []ErrorDetails  `json:"errors"`
	Statistics interface{}     `json:"statistics"`
}
