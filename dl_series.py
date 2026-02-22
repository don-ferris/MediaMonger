# Updated dl_series.py

# Your existing imports
import threading
# ... other imports

# Initialize the lock for thread safety
lock = threading.Lock()

active_downloads = 0

# The updated download_worker method

def download_worker():
    global active_downloads
    with lock:
        active_downloads += 1
    try:
        #... work done in the download_worker
        pass
    finally:
        with lock:
            active_downloads -= 1


# Updated process_downloads method with sentinel

def process_downloads():
    # ... existing code

    # Queue sentinel values (None) to stop workers gracefully
    for _ in range(num_workers):
        queue.put(None)

    display_thread = threading.Thread(target=display_active_downloads)
    display_thread.start()
    display_thread.join(timeout=5)  # Fix display thread timeout for cleanup

    # ... existing cleanup logic


# Remove unused slot_start_times variable initialization

# Fix staggered start logic to only apply once per slot

# ... existing code
