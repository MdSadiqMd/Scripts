package models

type ClerkUser struct {
	FirstName      string `json:"first_name"`
	LastName       string `json:"last_name"`
	Username       string `json:"username"`
	EmailAddresses []struct {
		EmailAddress string `json:"email_address"`
	} `json:"email_addresses"`
}

type Entry struct {
	Row    int
	UserID string
}

type Result struct {
	Row      int
	UserName string
	Err      error
}
