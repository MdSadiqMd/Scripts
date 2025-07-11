// A script that reads an Excel file, extracts Clerk user IDs from a specified column, fetches user names concurrently via the Clerk API, and writes an updated Excel file with the resolved usernames in a new column
// go run main.go <path_of_excel_file>
package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/MdSadiqMd/clerk-to-usernames-excel/internal/models"
	"github.com/MdSadiqMd/clerk-to-usernames-excel/pkg"
	"github.com/xuri/excelize/v2"
)

const (
	userIDsColumnIndex = 5  // Coloumn in excel which have userIds
	batchSize          = 10 // Size of Batches that needed to be processed at a time
	ClerkSecretKey     = "" // Clerk Secret Key
	clerkAPIBase       = "https://api.clerk.dev/v1/users"
	requestTimeout     = 10 * time.Second
)

func fetchUserName(userID, secretKey string) (string, error) {
	fmt.Println("🔍 Fetching user:", userID)
	url := fmt.Sprintf("%s/%s", clerkAPIBase, userID)
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("Authorization", "Bearer "+secretKey)

	client := &http.Client{Timeout: requestTimeout}
	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("clerk API returned status %d", resp.StatusCode)
	}

	var user models.ClerkUser
	body, _ := ioutil.ReadAll(resp.Body)
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

func stringTrim(s string) string {
	return string([]byte(s))
}

func processExcel(filePath, secretKey string) error {
	f, err := excelize.OpenFile(filePath)
	if err != nil {
		return err
	}
	defer f.Close()

	sheet := f.GetSheetName(0)
	rows, err := f.GetRows(sheet)
	if err != nil {
		return err
	}

	var entries []models.Entry
	for r := 1; r < len(rows); r++ {
		if len(rows[r]) >= userIDsColumnIndex {
			uid := stringTrim(rows[r][userIDsColumnIndex-1])
			if uid != "" {
				entries = append(entries, models.Entry{Row: r + 1, UserID: uid})
			}
		}
	}

	colName, _ := excelize.ColumnNumberToName(len(rows[0]) + 1)
	f.SetCellValue(sheet, fmt.Sprintf("%s1", colName), "User Name")
	sem := make(chan struct{}, batchSize)

	var wg sync.WaitGroup
	results := make(chan models.Result, len(entries))
	for _, ent := range entries {
		wg.Add(1)
		sem <- struct{}{}
		go func(e models.Entry) {
			defer wg.Done()
			name, err := fetchUserName(e.UserID, secretKey)
			if err != nil {
				name = fmt.Sprintf("Unknown User (%s)", e.UserID)
				fmt.Printf("⚠️ Failed to fetch %s: %v\n", e.UserID, err)
			}
			results <- models.Result{Row: e.Row, UserName: name}
			<-sem
		}(ent)
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	for res := range results {
		cell, _ := excelize.CoordinatesToCellName(len(rows[0])+1, res.Row)
		f.SetCellValue(sheet, cell, res.UserName)
	}

	ext := filepath.Ext(filePath)
	out := filePath[:len(filePath)-len(ext)] + "_updated" + ext
	if err := f.SaveAs(out); err != nil {
		return err
	}
	fmt.Printf("✅ Processing complete! Updated file saved as: %s\n", out)
	return nil
}

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Please provide the Excel file path")
		os.Exit(1)
	}

	filePath := os.Args[1]
	if err := pkg.ValidateFile(filePath); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	secretKey := ClerkSecretKey
	if secretKey == "" {
		fmt.Println("CLERK_SECRET_KEY not set")
		os.Exit(1)
	}

	if err := processExcel(filePath, secretKey); err != nil {
		fmt.Printf("Script failed: %v\n", err)
		os.Exit(1)
	}
}
