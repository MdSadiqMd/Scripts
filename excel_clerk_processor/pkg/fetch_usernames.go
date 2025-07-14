package pkg

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	models "github.com/MdSadiqMd/clerk-to-usernames-excel/internal"
)

func FetchUserName(userID, secretKey string) (string, error) {
	fmt.Println("🔍 Fetching user:", userID)
	url := fmt.Sprintf("%s/%s", models.CLERK_API, userID)
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("Authorization", "Bearer "+secretKey)

	client := &http.Client{Timeout: models.REQUEST_TIMEOUT}
	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("clerk API returned status %d", resp.StatusCode)
	}

	var user models.ClerkUser
	body, _ := io.ReadAll(resp.Body)
	if err := json.Unmarshal(body, &user); err != nil {
		return "", err
	}

	fullName := fmt.Sprintf("%s %s", user.FirstName, user.LastName)
	if trimmed := stringTrim(fullName); trimmed != "" {
		return trimmed, nil
	}
	if len(user.EmailAddresses) > 0 {
		return user.EmailAddresses[0].EmailAddress, nil
	}
	if user.Username != "" {
		return user.Username, nil
	}
	return "User " + userID, nil
}
