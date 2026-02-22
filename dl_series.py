#!/usr/bin/env python3
"""
dl_series.py - Download manager for TV series from Real-Debrid links
Downloads files to: ./series/[series_name]/
"""

import sys
import os
import subprocess
import time
import shutil
import threading
import signal
from datetime import datetime
from pathlib import Path
import urllib.parse
import queue
import fileinput
import re

def setup_signal_handlers():
    """Setup signal handlers to handle interruptions gracefully"""
    def signal_handler(sig, frame):
        print("\n\nInterrupted! Downloads will continue in background.")
        print("Check dl_series.log for progress.")
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

class DownloadManager:
    def __init__(self, series_input):
        # Check if input is a full path or just a name
        self.links_file = self.find_links_file(series_input)
        self.series_name = self.links_file.stem  # Remove .links extension
        self.series_dir = Path.cwd() / "series" / self.series_name
        self.backup_file = self.links_file.with_suffix('.links.bak')
        
        # Rotate log file before creating new one
        self.rotate_log_file()
        
        self.active_downloads = 0
        self.max_concurrent = 4
        self.download_queue = queue.Queue()
        self.lock = threading.Lock()
        
        # Progress tracking
        self.slot_status = [""] * 4  # Status for each of 4 slots
        self.slot_lock = threading.Lock()
        self.display_thread = None
        self.running = True
        
        # Track staggered starts
        self.slot_start_times = [0] * 4
        
        # Create series directory if it doesn't exist
        self.series_dir.mkdir(parents=True, exist_ok=True)
    
    def rotate_log_file(self):
        """Rename existing log file with timestamp if it exists"""
        log_file = Path.cwd() / "dl_series.log"
        if log_file.exists():
            timestamp = datetime.now().strftime("%y%m%d%H%M")
            new_name = f"dl_series-{self.series_name}.{timestamp}.log"
            new_log = Path.cwd() / new_name
            try:
                shutil.move(log_file, new_log)
            except Exception as e:
                print(f"Warning: Could not rotate log file: {e}")
        
        self.log_file = Path.cwd() / "dl_series.log"
    
    def find_links_file(self, series_input):
        """Find the links file, trying different possibilities"""
        input_path = Path(series_input)
        
        # Check if input is an absolute or relative path to a file
        if input_path.exists() and input_path.is_file():
            return input_path.resolve()
        
        # Check if input ends with .links
        if not series_input.endswith('.links'):
            # Try adding .links extension
            links_path = Path(f"{series_input}.links")
            if links_path.exists() and links_path.is_file():
                return links_path.resolve()
        
        # Check current directory for the file
        current_dir_file = Path.cwd() / series_input
        if current_dir_file.exists() and current_dir_file.is_file():
            return current_dir_file.resolve()
        
        # Check current directory with .links extension
        if not series_input.endswith('.links'):
            current_dir_links = Path.cwd() / f"{series_input}.links"
            if current_dir_links.exists() and current_dir_links.is_file():
                return current_dir_links.resolve()
        
        # If we get here, file doesn't exist
        raise FileNotFoundError(f"Cannot find '{series_input}' or '{series_input}.links'")
    
    def log(self, message, echo=False):
        """Log message with timestamp - NO CONSOLE OUTPUT"""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        # Clean the message of any problematic characters for logging
        cleaned_message = message.replace('\n', ' ').replace('\r', ' ').replace('\0', '')
        log_line = f"{timestamp} - {cleaned_message}"
        
        with open(self.log_file, 'a', encoding='utf-8') as f:
            f.write(log_line + "\n")
        
        # NO ECHO TO CONSOLE
    
    def validate_links_file(self):
        """Validate the links file exists and has content"""
        if not self.links_file.exists():
            raise FileNotFoundError(f"File '{self.links_file}' does not exist")
        
        if not self.links_file.name.endswith('.links'):
            raise ValueError("File must have '.links' extension")
        
        with open(self.links_file, 'r', encoding='utf-8') as f:
            content = f.read().strip()
            if not content:
                raise ValueError("Links file is empty")
        
        return True
    
    def create_backup(self):
        """Create backup of links file only if it doesn't exist"""
        if not self.backup_file.exists():
            shutil.copy2(self.links_file, self.backup_file)
        else:
            pass  # Do nothing if backup already exists
    
    def read_links(self):
        """Read all links from file, return lines and valid https links"""
        with open(self.links_file, 'r', encoding='utf-8') as f:
            lines = [line.rstrip('\n') for line in f.readlines()]
        
        # Find https links (not marked as FAILED or COMPLETE)
        valid_links = []
        for i, line in enumerate(lines):
            stripped = line.strip()
            # Check if line starts with https:// and doesn't have status prefix
            if stripped.startswith('https://'):
                valid_links.append((i, stripped))
        
        return lines, valid_links
    
    def update_line_status(self, line_number, new_status):
        """Update a specific line in the links file with fileinput"""
        # First, read the current line to understand its content
        with open(self.links_file, 'r', encoding='utf-8') as f:
            lines = f.readlines()
            if line_number >= len(lines):
                return False
            original_line = lines[line_number].rstrip('\n')
        
        # Prepare the new line
        if original_line.startswith('# '):
            # Remove existing status prefix
            parts = original_line.split('https://', 1)
            if len(parts) == 2:
                clean_line = 'https://' + parts[1]
            else:
                clean_line = original_line
        else:
            clean_line = original_line
        
        new_line = f"{new_status} {clean_line}"
        
        # Use fileinput for in-place editing (modifies only the target line)
        current_line = 0
        for line in fileinput.input(self.links_file, inplace=True):
            line = line.rstrip('\n')
            if current_line == line_number:
                print(new_line)
            else:
                print(line)
            current_line += 1
        
        return True
    
    def extract_filename_from_url(self, url):
        """Extract filename from URL (without decoding)"""
        # Clean the URL first - remove any whitespace or control characters
        url = url.strip()
        
        # Get filename from URL (last part after /)
        filename = url.split('/')[-1].split('?')[0]
        
        # If no filename in URL, generate a simple one
        if not filename or '.' not in filename:
            # Use a simple hash of the URL
            import hashlib
            url_hash = hashlib.md5(url.encode()).hexdigest()[:12]
            filename = f"download_{url_hash}"
        
        return filename
    
    def decode_filename(self, filename):
        """Decode URL-encoded characters in filename"""
        try:
            # First, clean the filename of any whitespace or control characters
            filename = filename.strip()
            # Decode URL-encoded characters
            decoded = urllib.parse.unquote(filename)
            # Clean any remaining whitespace or newlines
            decoded = decoded.replace('\n', ' ').replace('\r', ' ').replace('\0', '')
            # Remove any extra whitespace
            decoded = ' '.join(decoded.split())
            return decoded
        except:
            # If decoding fails, clean and return original
            filename = filename.replace('\n', ' ').replace('\r', ' ').replace('\0', '')
            filename = ' '.join(filename.split())
            return filename
    
    def get_expected_size(self, url):
        """Get expected file size from URL header - returns size in BYTES"""
        try:
            # Clean the URL first
            url = url.strip().replace('\n', ' ').replace('\r', ' ').replace('\0', '')
            url = ' '.join(url.split())
            
            self.log(f"Getting expected size for: {url}")
            
            # Use wget with spider mode to get file info
            cmd = ['wget', '--spider', '--server-response', '--timeout=10', '--tries=2', url]
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=15)
            
            # Parse Content-Length from output
            for line in result.stderr.split('\n'):
                if 'Content-Length:' in line:
                    size_str = line.split('Content-Length:')[1].strip()
                    try:
                        size_bytes = int(size_str)
                        self.log(f"Got expected size: {size_bytes:,} bytes")
                        return size_bytes
                    except ValueError:
                        continue
            
            # Try curl as fallback
            cmd = ['curl', '-s', '-I', '-L', '--max-time', '10', url]
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=15)
            
            for line in result.stdout.split('\n'):
                if 'content-length:' in line.lower():
                    size_str = line.split(':')[1].strip()
                    try:
                        size_bytes = int(size_str)
                        self.log(f"Got expected size: {size_bytes:,} bytes")
                        return size_bytes
                    except ValueError:
                        continue
                        
        except (subprocess.TimeoutExpired, subprocess.CalledProcessError, Exception) as e:
            self.log(f"Error getting expected size: {e}")
        
        self.log("Could not get expected file size")
        return None
    
    def check_existing_file(self, decoded_filename):
        """Check if file already exists - use DECODED filename"""
        filepath = Path.cwd() / decoded_filename
        
        if not filepath.exists():
            return False, None
        
        try:
            actual_size = filepath.stat().st_size
            return True, actual_size
                
        except OSError as e:
            return False, None
    
    def check_if_file_is_active_download(self, decoded_filename):
        """Check if file exists and is an active download by checking size changes - use DECODED filename"""
        filepath = Path.cwd() / decoded_filename
        
        if not filepath.exists():
            return False, None  # File doesn't exist
        
        # Get initial size
        try:
            size1 = filepath.stat().st_size
        except OSError:
            return False, None  # Can't stat file
        
        # Wait 10 seconds
        time.sleep(10)
        
        # Get size again
        try:
            size2 = filepath.stat().st_size
        except OSError:
            return False, None  # Can't stat file
        
        if size1 != size2:
            return True, size2  # File is actively downloading
        
        # Wait another 10 seconds
        time.sleep(10)
        
        # Get size one more time
        try:
            size3 = filepath.stat().st_size
        except OSError:
            return False, None
        
        if size2 != size3:
            return True, size3  # File is actively downloading
        
        # File size hasn't changed in 20 seconds - not an active download
        return False, size3
    
    def update_slot_status(self, slot_id, status):
        """Update status for a specific slot"""
        with self.slot_lock:
            self.slot_status[slot_id] = status
    
    def display_progress(self):
        """Display thread that shows progress at fixed intervals"""
        while self.running:
            with self.slot_lock:
                # Clear screen and show all 4 slots
                print("\033[2J\033[H", end='')  # Clear screen and move to home
                print("Download Progress:")
                print("-" * 60)
                for i, status in enumerate(self.slot_status):
                    if status:
                        print(status)
                    else:
                        print(f"{i+1}: [Waiting for download...]")
                sys.stdout.flush()
            
            time.sleep(0.5)  # Update twice per second
    
    def format_filename_for_display(self, filename):
        """Format filename for display: first 30 chars + ... + last 10 chars"""
        # Clean the filename first
        filename = filename.replace('\n', ' ').replace('\r', ' ').replace('\0', '')
        filename = ' '.join(filename.split())
        
        if len(filename) <= 43:  # 30 + 3 (...) + 10
            return filename
        
        first_part = filename[:30]
        last_part = filename[-10:]
        return f"{first_part}...{last_part}"
    
    def download_with_wget(self, url, decoded_filename, slot_id):
        """Download a file using wget with clean progress display - wget will decode filename automatically"""
        # Clean the URL and filename
        url = url.strip().replace('\n', ' ').replace('\r', ' ').replace('\0', '')
        url = ' '.join(url.split())
        decoded_filename = decoded_filename.strip().replace('\n', ' ').replace('\r', ' ').replace('\0', '')
        decoded_filename = ' '.join(decoded_filename.split())
        
        # Build wget command
        wget_cmd = [
            'wget', '-c',
            '--progress=dot:giga',
            '--show-progress',
            '--tries=3',
            '--timeout=30',
            '--waitretry=5',
            '--retry-connrefused',
            '--no-check-certificate',
            '--header', 'Accept: */*',
            '--user-agent', 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            '-O', decoded_filename,  # Specify output filename to ensure it uses decoded name
            url
        ]
        
        # Update slot status to show 0% at start
        display_filename = self.format_filename_for_display(decoded_filename)
        self.update_slot_status(slot_id, f"{slot_id+1}: {display_filename} [  0%]")
        
        # LOG: Starting download
        self.log(f"STARTING DOWNLOAD: {url}")
        self.log(f"Downloading to (decoded): {decoded_filename}")
        
        # Start wget process with captured output
        try:
            process = subprocess.Popen(
                wget_cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
                universal_newlines=True,
                cwd=str(Path.cwd())
            )
            
            # Monitor progress
            last_percent = -1
            first_update = True
            while True:
                if process.poll() is not None:
                    break
                
                # Read progress from stdout
                line = process.stdout.readline()
                if line:
                    # Parse wget progress output for percentage, speed, and ETA
                    percent_match = re.search(r'(\d+)%', line)
                    
                    if percent_match:
                        percent = int(percent_match.group(1))
                        
                        # Only update if percentage changed (to reduce flicker)
                        if percent != last_percent:
                            last_percent = percent
                            
                            # Try to extract download speed (look for patterns like "1.2M/s" or "1024k/s")
                            speed_match = re.search(r'([\d.]+\s*[KMGT]?B/s)', line, re.IGNORECASE)
                            speed = speed_match.group(1).strip() if speed_match else ""
                            
                            # Try to extract ETA
                            eta_match = re.search(r'ETA\s+(\d+:\d+:\d+|\d+:\d+|\d+s)', line)
                            if not eta_match:
                                # Try alternative pattern without "ETA"
                                eta_match = re.search(r'\s(\d+m\d+s|\d+s)\s*$', line)
                            
                            eta = eta_match.group(1) if eta_match else ""
                            
                            # Create a simple progress bar
                            bar_length = 20
                            filled = int(bar_length * percent / 100)
                            bar = '[' + '=' * filled + ' ' * (bar_length - filled) + ']'
                            
                            # Build progress message
                            progress_msg = f"{slot_id+1}: {display_filename}"
                            progress_msg += f" {bar} {percent}%"
                            if speed:
                                progress_msg += f" {speed}"
                            if eta:
                                progress_msg += f" ETA: {eta}"
                            
                            # Update the slot status
                            self.update_slot_status(slot_id, progress_msg)
                    
                    # If this is the first update and we see progress but no percentage yet,
                    # ensure we show 0%
                    elif first_update and ('Downloading' in line or 'Connecting' in line):
                        self.update_slot_status(slot_id, f"{slot_id+1}: {display_filename} [  0%]")
                        first_update = False
                
                time.sleep(0.1)  # Small delay
            
            # Wait for process to finish
            return_code = process.wait()
            
            filepath = Path.cwd() / decoded_filename
            if return_code == 0 and filepath.exists():
                # Get file size for completion message
                file_size = filepath.stat().st_size
                size_mb = file_size / (1024 * 1024)
                
                # LOG: Download completed
                self.log(f"DOWNLOAD COMPLETED: {url}")
                self.log(f"File: {decoded_filename}, Size: {file_size:,} bytes ({size_mb:.1f} MB)")
                
                # Update with completion status
                self.update_slot_status(slot_id, f"{slot_id+1}: {display_filename} [COMPLETE, {size_mb:.1f}MB]")
                return True, filepath
            else:
                self.log(f"DOWNLOAD FAILED: wget returned code {return_code} for {url}")
                self.update_slot_status(slot_id, f"{slot_id+1}: {display_filename} [FAILED]")
                return False, None
                
        except Exception as e:
            self.log(f"DOWNLOAD ERROR for {url}: {str(e)}")
            self.update_slot_status(slot_id, f"{slot_id+1}: {display_filename} [ERROR]")
            return False, None
    
    def verify_download(self, url, local_file, expected_size):
        """Verify downloaded file size matches expected size - detailed logging"""
        if not local_file or not local_file.exists():
            self.log(f"VERIFICATION FAILED: Local file not found for {url}")
            return False, 0
        
        actual_size = local_file.stat().st_size
        
        if expected_size:
            if actual_size == expected_size:
                self.log(f"VERIFICATION PASSED: Size match")
                self.log(f"  Local file size: {actual_size:,} bytes")
                self.log(f"  Expected file size: {expected_size:,} bytes")
                self.log(f"  Result: PASS - sizes match")
                return True, actual_size
            else:
                self.log(f"VERIFICATION FAILED: Size mismatch")
                self.log(f"  Local file size: {actual_size:,} bytes")
                self.log(f"  Expected file size: {expected_size:,} bytes")
                self.log(f"  Difference: {abs(actual_size - expected_size):,} bytes")
                
                # Calculate percentage difference
                if expected_size > 0:
                    percent_diff = abs(actual_size - expected_size) / expected_size * 100
                    self.log(f"  Difference: {percent_diff:.2f}%")
                
                self.log(f"  Result: FAIL - size mismatch")
                return False, actual_size
        else:
            self.log(f"VERIFICATION SKIPPED: No expected size available")
            self.log(f"  Local file size: {actual_size:,} bytes")
            self.log(f"  Result: PASS (no expected size to compare)")
            return True, actual_size  # If no expected size, assume OK
    
    def move_to_series_directory(self, local_file, decoded_filename):
        """Move downloaded file to series directory with decoded filename"""
        if not local_file or not local_file.exists():
            self.log(f"MOVE FAILED: Local file not found")
            return None
        
        # Create destination path with decoded filename
        destination = self.series_dir / decoded_filename
        
        # If file already exists at destination, add a counter
        counter = 1
        original_destination = destination
        while destination.exists():
            name_parts = decoded_filename.rsplit('.', 1)
            if len(name_parts) == 2:
                new_name = f"{name_parts[0]}_{counter}.{name_parts[1]}"
            else:
                new_name = f"{decoded_filename}_{counter}"
            destination = self.series_dir / new_name
            counter += 1
        
        try:
            shutil.move(str(local_file), str(destination))
            self.log(f"MOVED TO SERIES DIRECTORY: {local_file.name} -> {destination.name}")
            return destination
        except Exception as e:
            self.log(f"ERROR moving file to series directory: {e}")
            return None
    
    def process_download(self, line_num, url, slot_id):
        """Process a single download"""
        # Clean the URL first
        url = url.strip().replace('\n', ' ').replace('\r', ' ').replace('\0', '')
        url = ' '.join(url.split())
        
        # Extract filename from URL
        encoded_filename = self.extract_filename_from_url(url)
        decoded_filename = self.decode_filename(encoded_filename)
        
        self.log(f"Processing: {url}")
        self.log(f"  Encoded filename: {encoded_filename}")
        self.log(f"  Decoded filename: {decoded_filename}")
        
        # Check if file already exists in current directory - use DECODED filename
        file_exists, existing_size = self.check_existing_file(decoded_filename)
        
        if file_exists:
            self.log(f"Local file exists: {decoded_filename}")
            self.log(f"Local file size: {existing_size:,} bytes")
            
            # Now get expected file size for comparison
            self.log(f"Getting expected file size for comparison...")
            expected_size = self.get_expected_size(url)
            
            if expected_size is not None:
                self.log(f"Expected file size: {expected_size:,} bytes")
                self.log(f"Test condition: local size == expected size? {existing_size == expected_size}")
                
                if existing_size == expected_size:
                    self.log(f"Result: File already exists with correct size")
                    
                    # Move to series directory
                    local_file = Path.cwd() / decoded_filename
                    moved_file = self.move_to_series_directory(local_file, decoded_filename)
                    
                    if moved_file:
                        # Mark as complete
                        self.update_line_status(line_num, f"# COMPLETE (already existed)")
                        display_name = self.format_filename_for_display(decoded_filename)
                        size_mb = existing_size / (1024 * 1024)
                        self.update_slot_status(slot_id, f"{slot_id+1}: {display_name} [EXISTED, {size_mb:.1f}MB]")
                        return True, "ALREADY_EXISTED"
                    else:
                        self.log(f"Result: FAILED - could not move existing file")
                        self.update_line_status(line_num, f"# FAILED - could not move existing file")
                        return False, "MOVE_FAILED"
                else:
                    self.log(f"Result: File exists but size mismatch ({existing_size:,} != {expected_size:,})")
                    self.log(f"  Difference: {abs(existing_size - expected_size):,} bytes")
                    
                    # Calculate percentage difference for logging
                    if expected_size > 0:
                        percent_diff = abs(existing_size - expected_size) / expected_size * 100
                        self.log(f"  Difference: {percent_diff:.2f}%")
                    
                    # FILE SIZE MISMATCH - DO NOT MOVE THE FILE
                    self.log(f"  Action: NOT moving file - size mismatch indicates incomplete download")
                    self.update_line_status(line_num, f"# FAILED - size mismatch (local {existing_size:,} != expected {expected_size:,})")
                    return False, "SIZE_MISMATCH"
            else:
                self.log(f"Result: Could not get expected file size for comparison")
                # Cannot verify file - do not move it
                self.log(f"  Action: NOT moving file - cannot verify completeness")
                self.update_line_status(line_num, f"# FAILED - cannot verify file size")
                return False, "NO_EXPECTED_SIZE"
        else:
            self.log(f"No local file found: {decoded_filename}")
        
        # Check if file is actively downloading - use DECODED filename
        is_active, current_size = self.check_if_file_is_active_download(decoded_filename)
        
        if is_active:
            self.log(f"Result: SKIPPED - file actively downloading")
            display_name = self.format_filename_for_display(decoded_filename)
            self.update_slot_status(slot_id, f"{slot_id+1}: {display_name} [ACTIVE]")
            return False, "ACTIVE"
        
        # Get expected size for new download
        expected_size = self.get_expected_size(url)
        
        # Stagger start times
        if slot_id == 0:
            time.sleep(5)
        elif slot_id == 1:
            time.sleep(10)
        elif slot_id == 2:
            time.sleep(15)
        # Slot 3 (index 3) starts immediately
        
        # Start download - wget will save with decoded filename
        completed, local_file = self.download_with_wget(url, decoded_filename, slot_id)
        
        if not completed or not local_file:
            # Mark as failed
            self.update_line_status(line_num, f"# FAILED - ")
            self.log(f"Result: FAILED - download error")
            return False, "DOWNLOAD_FAILED"
        
        # Verify download
        verified, actual_size = self.verify_download(url, local_file, expected_size)
        
        if not verified:
            # Mark as failed with size mismatch
            self.update_line_status(line_num, f"# FAILED - ")
            self.log(f"Result: FAILED - verification failed (size mismatch)")
            # Delete the failed file
            try:
                local_file.unlink()
                self.log(f"Deleted failed file: {local_file}")
            except Exception as e:
                self.log(f"Error deleting failed file: {e}")
            return False, "VERIFICATION_FAILED"
        
        # Move to series directory
        moved_file = self.move_to_series_directory(local_file, decoded_filename)
        
        if not moved_file:
            # Mark as failed to move
            self.update_line_status(line_num, f"# FAILED - ")
            self.log(f"Result: FAILED - could not move to series directory")
            return False, "MOVE_FAILED"
        
        # Mark as complete
        self.update_line_status(line_num, f"# COMPLETE")
        self.log(f"Result: SUCCESS")
        return True, "COMPLETE"
    
    def download_worker(self, slot_id):
        """Worker thread that processes downloads from the queue"""
        while True:
            try:
                # Get next download from queue with timeout
                item = self.download_queue.get(timeout=5)
                if item is None:  # Sentinel value to stop worker
                    self.download_queue.task_done()
                    break
                
                line_num, url = item
                
                # Process the download
                success, status = self.process_download(line_num, url, slot_id)
                
                self.download_queue.task_done()
                
            except queue.Empty:
                # Check if we should exit
                with self.lock:
                    if self.active_downloads == 0 and self.download_queue.empty():
                        break
                continue
            except Exception as e:
                self.log(f"Error in download worker {slot_id}: {e}")
                self.download_queue.task_done()
    
    def process_downloads(self):
        """Main download processing loop using queue"""
        # Read initial links
        all_lines, valid_links = self.read_links()
        
        if not valid_links:
            self.log("No valid https:// links found in file")
            return
        
        self.log(f"Found {len(valid_links)} valid download links")
        self.log(f"Files will be saved to: {self.series_dir}")
        
        # Start display thread
        self.display_thread = threading.Thread(target=self.display_progress)
        self.display_thread.daemon = True
        self.display_thread.start()
        
        # Wait a moment for display to initialize
        time.sleep(0.5)
        
        # Add all initial links to queue
        for link_info in valid_links:
            self.download_queue.put(link_info)
        
        # Start worker threads with assigned slot IDs
        workers = []
        for i in range(self.max_concurrent):
            worker = threading.Thread(target=self.download_worker, args=(i,))
            worker.daemon = True
            worker.start()
            workers.append(worker)
        
        # Monitor for completion
        while True:
            # Check if all work is done
            with self.lock:
                if self.active_downloads == 0 and self.download_queue.empty():
                    # Wait a bit to be sure
                    time.sleep(3)
                    break
            
            time.sleep(2)
        
        # Stop display thread
        self.running = False
        if self.display_thread:
            self.display_thread.join(timeout=1)
        
        # Wait for all workers to finish
        for worker in workers:
            worker.join(timeout=10)
        
        # Final display
        print("\033[2J\033[H", end='')
        print("All downloads completed!")
        self.log("All downloads processed!")
    
    def run(self):
        """Run the download manager"""
        try:
            self.validate_links_file()
        except Exception as e:
            print(f"Error: {e}")
            print("\nUsage: dl_series.py '[series name]' or dl_series.py '[series name].links'")
            print("       This can be either a series name or a .links file in the")
            print("       current directory containing multiple real-debrid links")
            print("       for downloading a TV series.")
            print("\nExamples:")
            print("  ./dl_series.py house              (looks for house.links)")
            print("  ./dl_series.py house.links        (uses house.links directly)")
            print("  ./dl_series.py ./path/house.links (uses full path)")
            print(f"\nDownloads will be saved to: ./series/{self.series_name}/")
            sys.exit(1)
        
        self.log(f"Starting download session for {self.links_file.name}")
        self.log(f"Series directory: {self.series_dir}")
        self.create_backup()
        
        self.process_downloads()
        
        # Call reneps.py from current directory, passing series name
        reneps_script = Path.cwd() / "reneps.py"
        if reneps_script.exists():
            self.log("All downloads complete. Calling reneps.py...")
            try:
                # Call reneps.py from current directory, passing the series name
                subprocess.run([sys.executable, str(reneps_script), self.series_name], 
                             check=True, cwd=Path.cwd())
                self.log("reneps.py completed successfully")
            except subprocess.CalledProcessError as e:
                self.log(f"Error running reneps.py: {e}")
            except FileNotFoundError:
                self.log("reneps.py not found")
        else:
            self.log("Note: reneps.py not found in current directory")

def main():
    """Main entry point"""
    setup_signal_handlers()
    
    if len(sys.argv) != 2:
        print("Usage: dl_series.py '[series name]' or dl_series.py '[series name].links'")
        print("       This can be either a series name or a .links file in the")
        print("       current directory containing multiple real-debrid links")
        print("       for downloading a TV series.")
        print("\nExamples:")
        print("  ./dl_series.py house              (looks for house.links)")
        print("  ./dl_series.py house.links        (uses house.links directly)")
        print("  ./dl_series.py ./path/house.links (uses full path)")
        print("\nDownloads will be saved to: ./series/[series_name]/")
        sys.exit(1)
    
    series_input = sys.argv[1]
    
    # Initialize and run download manager
    try:
        manager = DownloadManager(series_input)
        manager.run()
    except FileNotFoundError as e:
        print(f"Error: {e}")
        print("\nUsage: dl_series.py '[series name]' or dl_series.py '[series name].links'")
        print("       This can be either a series name or a .links file in the")
        print("       current directory containing multiple real-debrid links")
        print("       for downloading a TV series.")
        print("\nExamples:")
        print("  ./dl_series.py house              (looks for house.links)")
        print("  ./dl_series.py house.links        (uses house.links directly)")
        print("  ./dl_series.py ./path/house.links (uses full path)")
        sys.exit(1)

if __name__ == "__main__":
    main()
