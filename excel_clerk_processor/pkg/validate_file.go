package pkg

import (
	"errors"
	"os"
	"path/filepath"
)

func ValidateFile(path string) error {
	info, err := os.Stat(path)
	if os.IsNotExist(err) || info.IsDir() {
		return errors.New("file not found or is a directory")
	}

	ext := filepath.Ext(path)
	if ext != ".xlsx" && ext != ".xls" {
		return errors.New("file must be an Excel file (.xlsx or .xls)")
	}
	return nil
}
