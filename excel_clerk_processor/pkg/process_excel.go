package pkg

import (
	"fmt"
	"path/filepath"
	"sync"

	models "github.com/MdSadiqMd/clerk-to-usernames-excel/internal"
	"github.com/xuri/excelize/v2"
)

func ProcessExcel(filePath, secretKey string) error {
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
		if len(rows[r]) >= models.USERIDs_COLOUMN {
			uid := stringTrim(rows[r][models.USERIDs_COLOUMN-1])
			if uid != "" {
				entries = append(entries, models.Entry{Row: r + 1, UserID: uid})
			}
		}
	}

	colName, _ := excelize.ColumnNumberToName(len(rows[0]) + 1)
	f.SetCellValue(sheet, fmt.Sprintf("%s1", colName), "User Name")
	sem := make(chan struct{}, models.BATCH_SIZE)

	var wg sync.WaitGroup
	results := make(chan models.Result, len(entries))
	for _, ent := range entries {
		wg.Add(1)
		sem <- struct{}{}
		go func(e models.Entry) {
			defer wg.Done()
			name, err := FetchUserName(e.UserID, secretKey)
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
