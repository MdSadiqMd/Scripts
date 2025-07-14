// A script that reads an Excel file, extracts Clerk user IDs from a specified column, fetches user names concurrently via the Clerk API, and writes an updated Excel file with the resolved usernames in a new column
// Add CLERK_SECRET_KEY in internal/constants.go
// go run main.go <path_of_excel_file>
package main

import (
	"fmt"
	"os"

	models "github.com/MdSadiqMd/clerk-to-usernames-excel/internal"
	"github.com/MdSadiqMd/clerk-to-usernames-excel/pkg"
)

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

	secretKey := models.CLERK_SECRET_KEY
	if secretKey == "" {
		fmt.Println("CLERK_SECRET_KEY not set")
		os.Exit(1)
	}

	if err := pkg.ProcessExcel(filePath, secretKey); err != nil {
		fmt.Printf("Script failed: %v\n", err)
		os.Exit(1)
	}
}
