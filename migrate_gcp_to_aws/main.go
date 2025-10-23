package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"cloud.google.com/go/storage"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"google.golang.org/api/iterator"
)

// Configuration struct
type Config struct {
	GCSBucket          string    `json:"gcs_bucket"`
	S3Bucket           string    `json:"s3_bucket"`
	AWSCredentialsFile string    `json:"aws_credentials_file"`
	AWSRegion          string    `json:"aws_region"`
	LogFile            string    `json:"log_file"`
	CutoffDate         time.Time `json:"-"`
	CutoffDateStr      string    `json:"cutoff_date"`
	MaxWorkers         int       `json:"max_workers"`
	VideoExtensions    []string  `json:"video_extensions"`
}

// LoadConfig loads configuration from JSON file or returns defaults
func LoadConfig(configPath string) (*Config, error) {
	config := &Config{
		GCSBucket:          "",
		S3Bucket:           "",
		AWSCredentialsFile: "/home/sadiq/projects/scripts/migrate_gcp_to_aws/.aws/credentials",
		AWSRegion:          "",
		LogFile:            "/home/sadiq/projects/scripts/migrate_gcp_to_aws/logs/migrate_gcp_to_s3.log",
		CutoffDateStr:      "2025-09-07",
		MaxWorkers:         20,
		VideoExtensions:    []string{".mp4", ".avi", ".mov", ".mkv", ".webm", ".m4v"},
	}

	// Try to load from config file if it exists
	if configPath != "" {
		data, err := os.ReadFile(configPath)
		if err == nil {
			if err := json.Unmarshal(data, config); err != nil {
				return nil, fmt.Errorf("failed to parse config file: %w", err)
			}
		}
	}

	// Parse cutoff date
	cutoffDate, err := time.Parse("2006-01-02", config.CutoffDateStr)
	if err != nil {
		return nil, fmt.Errorf("failed to parse cutoff date: %w", err)
	}
	config.CutoffDate = cutoffDate

	return config, nil
}

// Stats for tracking progress
type Stats struct {
	totalFiles      atomic.Int64
	copiedFiles     atomic.Int64
	skippedExisting atomic.Int64
	errorFiles      atomic.Int64
}

// FileJob represents a file to be migrated
type FileJob struct {
	GCSPath      string
	RelativePath string
	CreatedTime  time.Time
}

// Logger with timestamp
type TimestampLogger struct {
	logger *log.Logger
	mu     sync.Mutex
}

func NewTimestampLogger(logFile string) (*TimestampLogger, error) {
	f, err := os.OpenFile(logFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open log file: %w", err)
	}

	// Write to both file and stdout
	multiWriter := io.MultiWriter(os.Stdout, f)
	logger := log.New(multiWriter, "", 0)

	return &TimestampLogger{logger: logger}, nil
}

func (tl *TimestampLogger) Log(format string, v ...interface{}) {
	tl.mu.Lock()
	defer tl.mu.Unlock()
	timestamp := time.Now().Format("2006-01-02 15:04:05")
	message := fmt.Sprintf(format, v...)
	tl.logger.Printf("%s - %s", timestamp, message)
}

func (tl *TimestampLogger) Close() {
	// The file will be closed when the program exits
}

// Check if file extension is a video
func isVideoFile(filename string, extensions []string) bool {
	ext := strings.ToLower(filepath.Ext(filename))
	for _, validExt := range extensions {
		if ext == validExt {
			return true
		}
	}
	return false
}

// Extract date from folder path (e.g., "port1/2025-09-07/file.mp4" -> "2025-09-07")
// Path format: port1/2025-07-15/recording_...
func extractDateFromPath(path string) (time.Time, error) {
	// Get the second part of the path (date folder)
	parts := strings.Split(path, "/")
	if len(parts) < 2 {
		return time.Time{}, fmt.Errorf("path does not have enough segments: %s", path)
	}

	// Try to parse the second part as a date (YYYY-MM-DD format)
	dateStr := parts[1]
	date, err := time.Parse("2006-01-02", dateStr)
	if err != nil {
		return time.Time{}, fmt.Errorf("folder name is not a valid date: %s", dateStr)
	}

	return date, nil
}

// Check if file exists in S3
func fileExistsInS3(ctx context.Context, s3Client *s3.S3, bucket, key string) bool {
	_, err := s3Client.HeadObjectWithContext(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	return err == nil
}

// Worker function to process files
func worker(
	ctx context.Context,
	id int,
	jobs <-chan FileJob,
	config *Config,
	gcsClient *storage.Client,
	s3Client *s3.S3,
	uploader *s3manager.Uploader,
	stats *Stats,
	logger *TimestampLogger,
	wg *sync.WaitGroup,
) {
	defer wg.Done()

	for job := range jobs {
		stats.totalFiles.Add(1)
		current := stats.totalFiles.Load()

		logger.Log("Worker %d - [%d] Processing: %s (dated %s)",
			id, current, job.RelativePath, job.CreatedTime.Format("2006-01-02"))

		// Check if file already exists in S3
		if fileExistsInS3(ctx, s3Client, config.S3Bucket, job.RelativePath) {
			logger.Log("  Worker %d - ⊘ File already exists in S3, skipping", id)
			stats.skippedExisting.Add(1)
			continue
		}

		// Open GCS file
		gcsObj := gcsClient.Bucket(config.GCSBucket).Object(job.GCSPath)
		reader, err := gcsObj.NewReader(ctx)
		if err != nil {
			logger.Log("  Worker %d - ✗ Error opening GCS file: %v", id, err)
			stats.errorFiles.Add(1)
			continue
		}

		// Get file size for logging
		attrs, _ := gcsObj.Attrs(ctx)
		var sizeStr string
		if attrs != nil {
			sizeMB := float64(attrs.Size) / (1024 * 1024)
			sizeStr = fmt.Sprintf(" (%.2f MB)", sizeMB)
		}

		// Upload to S3
		logger.Log("  Worker %d - ⬆ Copying to S3%s...", id, sizeStr)
		startTime := time.Now()
		_, err = uploader.UploadWithContext(ctx, &s3manager.UploadInput{
			Bucket: aws.String(config.S3Bucket),
			Key:    aws.String(job.RelativePath),
			Body:   reader,
		})
		reader.Close()
		duration := time.Since(startTime)

		if err != nil {
			logger.Log("  Worker %d - ✗ Error uploading to S3: %v", id, err)
			stats.errorFiles.Add(1)
			continue
		}

		copied := stats.copiedFiles.Add(1)
		logger.Log("  Worker %d - ✓ Successfully copied in %.1fs (total: %d files)",
			id, duration.Seconds(), copied)
	}
}

func main() {
	// Load configuration (tries migrate_config.json first, falls back to defaults)
	configPath := "migrate_config.json"
	config, err := LoadConfig(configPath)
	if err != nil {
		log.Fatalf("Failed to load configuration: %v", err)
	}

	// Create log directory
	logDir := filepath.Dir(config.LogFile)
	if err := os.MkdirAll(logDir, 0755); err != nil {
		log.Fatalf("Failed to create log directory: %v", err)
	}

	// Initialize logger
	logger, err := NewTimestampLogger(config.LogFile)
	if err != nil {
		log.Fatalf("Failed to initialize logger: %v", err)
	}

	ctx := context.Background()

	// Initialize GCS client
	logger.Log("Initializing GCS client...")
	gcsClient, err := storage.NewClient(ctx)
	if err != nil {
		logger.Log("Failed to create GCS client: %v", err)
		logger.Log("Please run: gcloud auth application-default login")
		os.Exit(1)
	}
	defer gcsClient.Close()

	// Initialize AWS session
	logger.Log("Initializing AWS session...")
	sess, err := session.NewSession(&aws.Config{
		Region:      aws.String(config.AWSRegion),
		Credentials: credentials.NewSharedCredentials(config.AWSCredentialsFile, "default"),
	})
	if err != nil {
		logger.Log("Failed to create AWS session: %v", err)
		os.Exit(1)
	}

	s3Client := s3.New(sess)

	// Configure uploader for better performance
	uploader := s3manager.NewUploader(sess, func(u *s3manager.Uploader) {
		u.PartSize = 10 * 1024 * 1024 // 10MB parts (default is 5MB)
		u.Concurrency = 5             // Upload 5 parts concurrently per file
		u.LeavePartsOnError = false   // Clean up failed uploads
	})

	logger.Log("Starting migration from GCS to S3...")
	logger.Log("Cutoff date: %s (only copying files from this date onwards)", config.CutoffDate.Format("2006-01-02"))
	logger.Log("Source: gs://%s", config.GCSBucket)
	logger.Log("Destination: s3://%s", config.S3Bucket)
	logger.Log("Max concurrent workers: %d", config.MaxWorkers)

	// Create job channel and stats
	jobs := make(chan FileJob, config.MaxWorkers*2)
	stats := &Stats{}

	// Start workers
	var wg sync.WaitGroup
	for i := 1; i <= config.MaxWorkers; i++ {
		wg.Add(1)
		go worker(ctx, i, jobs, config, gcsClient, s3Client, uploader, stats, logger, &wg)
	}

	// List all objects in GCS bucket and send to workers
	bucket := gcsClient.Bucket(config.GCSBucket)
	query := &storage.Query{Prefix: ""}
	it := bucket.Objects(ctx, query)

	filesQueued := 0
	skippedByDate := 0
	totalProcessed := 0

	logger.Log("Scanning GCS bucket and queuing eligible files...")
	logger.Log("(Files before %s will be skipped)", config.CutoffDate.Format("2006-01-02"))
	logger.Log("")

	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			logger.Log("Error listing GCS objects: %v", err)
			break
		}

		// Skip directories
		if strings.HasSuffix(attrs.Name, "/") {
			continue
		}

		// Check if it's a video file
		if !isVideoFile(attrs.Name, config.VideoExtensions) {
			continue
		}

		totalProcessed++
		logger.Log("Scanning [%d]: %s", totalProcessed, attrs.Name)

		// Check folder date (primary filter)
		folderDate, err := extractDateFromPath(attrs.Name)
		if err != nil {
			logger.Log("  ✗ Skipped: Could not extract valid date from path (%v)", err)
			skippedByDate++
			continue
		}

		// Use folder date for filtering
		if folderDate.Before(config.CutoffDate) {
			logger.Log("  ✗ Skipped: File dated %s (before %s)",
				folderDate.Format("2006-01-02"), config.CutoffDate.Format("2006-01-02"))
			skippedByDate++
			continue
		}

		logger.Log("  ✓ Eligible: File dated %s - queuing for copy", folderDate.Format("2006-01-02"))

		// Create job
		job := FileJob{
			GCSPath:      attrs.Name,
			RelativePath: attrs.Name,
			CreatedTime:  folderDate,
		}

		jobs <- job
		filesQueued++
	}

	// Close jobs channel and wait for workers to finish
	close(jobs)
	logger.Log("")
	logger.Log("=== Scanning Complete ===")
	logger.Log("Total video files scanned: %d", totalProcessed)
	logger.Log("Files skipped (before cutoff date): %d", skippedByDate)
	logger.Log("Files queued for copying: %d", filesQueued)
	logger.Log("")
	logger.Log("=== Starting File Copy (20 workers in parallel) ===")
	logger.Log("")

	// Start a progress monitor
	done := make(chan bool)
	startProcessingTime := time.Now()
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				elapsed := time.Since(startProcessingTime)
				processed := stats.totalFiles.Load()
				rate := float64(processed) / elapsed.Seconds()
				logger.Log("")
				logger.Log("⏱ Progress Update (%.0fs elapsed, %.1f files/sec):", elapsed.Seconds(), rate)
				logger.Log("   Processed: %d/%d files", processed, filesQueued)
				logger.Log("   ✓ Copied: %d", stats.copiedFiles.Load())
				logger.Log("   ⊘ Skipped (already exist): %d", stats.skippedExisting.Load())
				logger.Log("   ✗ Errors: %d", stats.errorFiles.Load())
				logger.Log("")
			case <-done:
				return
			}
		}
	}()

	wg.Wait()
	done <- true
	totalDuration := time.Since(startProcessingTime)

	// Print statistics
	logger.Log("")
	logger.Log("========================================")
	logger.Log("           MIGRATION COMPLETE           ")
	logger.Log("========================================")
	logger.Log("")
	logger.Log("Scanning Phase:")
	logger.Log("  Total video files scanned: %d", totalProcessed)
	logger.Log("  Files skipped (before cutoff %s): %d", config.CutoffDate.Format("2006-01-02"), skippedByDate)
	logger.Log("  Files queued for copying: %d", filesQueued)
	logger.Log("")
	logger.Log("Processing Phase:")
	logger.Log("  Total files processed: %d", stats.totalFiles.Load())
	logger.Log("  ✓ Files copied to S3: %d", stats.copiedFiles.Load())
	logger.Log("  ⊘ Files skipped (already exist): %d", stats.skippedExisting.Load())
	logger.Log("  ✗ Errors: %d", stats.errorFiles.Load())
	logger.Log("")
	logger.Log("Performance:")
	logger.Log("  Total time: %.1f seconds (%.1f minutes)", totalDuration.Seconds(), totalDuration.Minutes())
	if stats.copiedFiles.Load() > 0 {
		avgTime := totalDuration.Seconds() / float64(stats.copiedFiles.Load())
		logger.Log("  Average time per file: %.1f seconds", avgTime)
		logger.Log("  Processing rate: %.2f files/second", float64(stats.totalFiles.Load())/totalDuration.Seconds())
	}
	logger.Log("")
	logger.Log("========================================")
}
