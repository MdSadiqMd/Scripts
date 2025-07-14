package models

import "time"

const (
	USERIDs_COLOUMN  = 5  // Coloumn in excel which have userIds
	BATCH_SIZE       = 10 // Size of Batches that needed to be processed at a time
	CLERK_SECRET_KEY = "" // Clerk Secret Key
	CLERK_API        = "https://api.clerk.dev/v1/users"
	REQUEST_TIMEOUT  = 10 * time.Second
)
