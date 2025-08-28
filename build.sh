#!/bin/bash
# Install system dependencies
apt-get update && apt-get install -y ffmpeg

# Install Python dependencies  
pip install -r requirements.txt

# Decrypt cookies if password provided
if [ ! -z "$COOKIES_PASSWORD" ]; then
    openssl enc -aes-256-cbc -d -in youtube_cookies.enc -out youtube_cookies.txt -k "$COOKIES_PASSWORD" -pbkdf2
fi
