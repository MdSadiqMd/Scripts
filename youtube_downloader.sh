#!/bin/bash

QUALITY="360" # change quality, when fails
START_TIME="00:11:51"
END_TIME="00:11:59"
VIDEO_URL="https://youtu.be/MzJ2RqFKJxw"
DEST_FOLDER="$(dirname "$0")"

mkdir -p "$DEST_FOLDER"
FILENAME="$DEST_FOLDER/%(title)s.%(ext)s"

yt-dlp \
    -f "bestvideo[height<=$QUALITY]+bestaudio/best[height<=$QUALITY]" \
    --download-sections "*${START_TIME}-${END_TIME}" \
    -o "$FILENAME" \
    "$VIDEO_URL"

echo "Download completed! Video saved to $DEST_FOLDER"
