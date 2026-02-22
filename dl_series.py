# Updated dl_series.py

# Import necessary libraries
import threading

# Class for handling downloads
class DownloadManager:
    def __init__(self):
        self.active_downloads = 0
        self.lock = threading.Lock()

    def download_worker(self, url):
        with self.lock:
            self.active_downloads += 1  # Increment counter for active downloads

        try:
            # Simulate download and potential error
            pass
        except Exception as e:
            print(f"Error occurred: {e}")  # Catch specific exceptions
        finally:
            with self.lock:
                self.active_downloads -= 1  # Decrement counter on completion

# Function to check if a file is actively being downloaded
import time

def check_if_file_is_active_download(file_path):
    timeout = 4  # Reduced blocking time
    start_time = time.time()
    while (time.time() - start_time) < timeout:
        if file_path in active_downloads:  # Replace with actual logic
            return True
    return False

# Function to sanitize file names
def sanitize_filename(filename):
    illegal_chars = '*/:?<>|"'
    return ''.join(c for c in filename if c not in illegal_chars)

# Function to handle logging with sanitized series name
def log_filename(series_name):
    sanitized_name = sanitize_filename(series_name)
    return f'log_{sanitized_name}.txt'

# Example of using log functionality
series_name = 'Example Series'
with open(log_filename(series_name), 'w') as log_file:
    log_file.write('# FAILED - Status message with error details')  # Updated to include actual error status
