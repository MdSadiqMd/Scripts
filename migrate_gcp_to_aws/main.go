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
		GCSBucket:          "",                                                                           // GCS bucket name
		S3Bucket:           "",                                                                           // S3 bucket name
		AWSCredentialsFile: "/home/sadiq/projects/scripts/migrate_gcp_to_aws/.aws/credentials",           // AWS credentials file path
		AWSRegion:          "ap-south-1",                                                                 // AWS region
		LogFile:            "/home/sadiq/projects/scripts/migrate_gcp_to_aws/logs/migrate_gcp_to_s3.log", // log file path
		CutoffDateStr:      "2025-09-07",                                                                 // date from which to migrate files
		MaxWorkers:         10,
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
	skippedDate     atomic.Int64
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

// Extract date from folder path (e.g., "2025-09-07/file.mp4" -> "2025-09-07")
func extractDateFromPath(path string) (time.Time, error) {
	// Get the first part of the path (folder name)
	parts := strings.Split(path, "/")
	if len(parts) == 0 {
		return time.Time{}, fmt.Errorf("empty path")
	}

	// Try to parse the first part as a date (YYYY-MM-DD format)
	dateStr := parts[0]
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

		logger.Log("Worker %d - Processing [%d]: %s", id, current, job.RelativePath)

		// Check if file already exists in S3
		if fileExistsInS3(ctx, s3Client, config.S3Bucket, job.RelativePath) {
			logger.Log("  Worker %d - File already exists in S3, skipping", id)
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

		// Upload to S3
		logger.Log("  Worker %d - Copying to s3://%s/%s...", id, config.S3Bucket, job.RelativePath)
		_, err = uploader.UploadWithContext(ctx, &s3manager.UploadInput{
			Bucket: aws.String(config.S3Bucket),
			Key:    aws.String(job.RelativePath),
			Body:   reader,
		})
		reader.Close()

		if err != nil {
			logger.Log("  Worker %d - ✗ Error uploading to S3: %v", id, err)
			stats.errorFiles.Add(1)
			continue
		}

		logger.Log("  Worker %d - ✓ Successfully copied", id)
		stats.copiedFiles.Add(1)
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
	uploader := s3manager.NewUploader(sess)

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

		// Check folder date (primary filter)
		folderDate, err := extractDateFromPath(attrs.Name)
		if err != nil {
			// If folder name is not a date, fall back to file creation time
			if attrs.Created.Before(config.CutoffDate) {
				skippedByDate++
				continue
			}
		} else {
			// Use folder date for filtering
			if folderDate.Before(config.CutoffDate) {
				skippedByDate++
				continue
			}
		}

		// Create job
		job := FileJob{
			GCSPath:      attrs.Name,
			RelativePath: attrs.Name,
			CreatedTime:  attrs.Created,
		}

		jobs <- job
		filesQueued++
	}

	logger.Log("Skipped %d files before cutoff date %s", skippedByDate, config.CutoffDate.Format("2006-01-02"))

	// Close jobs channel and wait for workers to finish
	close(jobs)
	logger.Log("Queued %d files for processing, waiting for workers to complete...", filesQueued)
	wg.Wait()

	// Print statistics
	logger.Log("================================")
	logger.Log("Migration completed")
	logger.Log("Total files processed: %d", stats.totalFiles.Load())
	logger.Log("Files copied: %d", stats.copiedFiles.Load())
	logger.Log("Files skipped (already exist): %d", stats.skippedExisting.Load())
	logger.Log("Files skipped (before cutoff date): %d", stats.skippedDate.Load())
	logger.Log("Errors: %d", stats.errorFiles.Load())
	logger.Log("================================")
}
