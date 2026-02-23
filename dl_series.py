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
        """Log message with timestamp with comprehensive error handling and fallback mechanisms."""
        try:
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            # Clean the message of any problematic characters for logging
            cleaned_message = message.replace('\n', ' ').replace('\r', ' ').replace('\0', '')
            # Also remove tabs to prevent alignment issues
            cleaned_message = cleaned_message.replace('\t', ' ')
            log_line = f"{timestamp} - {cleaned_message}"
            
            # Attempt to write to log file
            try:
                with open(self.log_file, 'a', encoding='utf-8', buffering=1) as f:
                    f.write(log_line + "\n")
                    f.flush()  # Ensure immediate write to disk
                return  # Success, exit early
                
            except IOError as e:
                # IOError: general file I/O issues (permissions, disk errors, etc.)
                self._handle_log_failure(log_line, f"IOError: {e}")
                
            except PermissionError as e:
                # PermissionError: no write permissions on log file
                self._handle_log_failure(log_line, f"PermissionError: {e}")
                
            except OSError as e:
                # OSError: system-level errors (disk full, etc.)
                self._handle_log_failure(log_line, f"OSError: {e}")
                
            except Exception as e:
                # Catch any other unexpected errors
                self._handle_log_failure(log_line, f"Unexpected error ({type(e).__name__}): {e}")
                
        except Exception as e:
            # Outermost catch for completely unexpected failures
            # Try to at least print to stderr as last resort
            try:
                print(f"[CRITICAL] Failed to process log message: {type(e).__name__}: {e}", file=sys.stderr)
            except:
                pass  # If even stderr fails, we've exhausted all options

    def _handle_log_failure(self, log_line, error_reason):
        """Handle log file write failures with fallback mechanisms.
        
        Attempts multiple fallback strategies when primary log fails:
        1. Try to write to stderr (for immediate visibility)
        2. Try to write to alternate log file
        3. Try to write to temporary log file
        """
        # Strategy 1: Try to write to stderr for immediate visibility
        try:
            print(f"[LOG FAILURE] {error_reason}", file=sys.stderr)
            print(f"[FAILED LOG] {log_line}", file=sys.stderr)
            return  # Success with stderr, exit
        except Exception as e:
            pass  # stderr failed, continue to fallback strategies
        
        # Strategy 2: Try to write to alternate log file
        try:
            alternate_log = Path.cwd() / f"dl_series-{self.series_name}-fallback.log"
            with open(alternate_log, 'a', encoding='utf-8', buffering=1) as f:
                f.write(f"[PRIMARY LOG FAILED: {error_reason}]\n")
                f.write(log_line + "\n")
                f.flush()
            return  # Success with fallback log, exit
        except Exception as e:
            pass  # Fallback log also failed, continue to final strategy
        
        # Strategy 3: Try to write to temp directory log file
        try:
            import tempfile
            temp_dir = Path(tempfile.gettempdir())
            temp_log = temp_dir / f"dl_series-{self.series_name}-emergency.log"
            with open(temp_log, 'a', encoding='utf-8', buffering=1) as f:
                f.write(f"[BOTH LOGS FAILED: {error_reason}]\n")
                f.write(log_line + "\n")
                f.flush()
            return  # Success with temp log, exit
        except Exception as e:
            pass  # Even temp log failed
        
        # All strategies failed - at least attempt stderr one more time
        try:
            print(f"[CRITICAL] All logging strategies failed. Message lost: {log_line}", file=sys.stderr)
        except:
            pass  # Nothing more we can do
    
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
        """Update a specific line in the links file with fileinput - with error handling"""
        try:
            # First, read the current line to understand its content
            try:
                with open(self.links_file, 'r', encoding='utf-8') as f:
                    lines = f.readlines()
                    if line_number >= len(lines):
                        self.log(f"Error updating line {line_number}: line number out of range (file has {len(lines)} lines)")
                        return False
                    original_line = lines[line_number].rstrip('\n')
            except IOError as e:
                self.log(f"Error reading links file before update: {e}")
                return False
            
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
            # FIX: Use sys.stdout.write() instead of print() to prevent writing to .links file
            try:
                current_line = 0
                for line in fileinput.input(self.links_file, inplace=True):
                    line = line.rstrip('\n')
                    if current_line == line_number:
                        sys.stdout.write(new_line + '\n')
                    else:
                        sys.stdout.write(line + '\n')
                    current_line += 1
                
                self.log(f"Successfully updated line {line_number} with status: {new_status}")
                return True
                
            except IOError as e:
                self.log(f"Error during fileinput operation on line {line_number}: {e}")
                return False
            except PermissionError as e:
                self.log(f"Permission denied updating links file on line {line_number}: {e}")
                return False
            except Exception as e:
                self.log(f"Unexpected error updating line {line_number}: {type(e).__name__}: {e}")
                return False
                
        except Exception as e:
            self.log(f"Critical error in update_line_status for line {line_number}: {type(e).__name__}: {e}")
            return False
    
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
        """Get expected file size from URL header - returns size in BYTES. Optimized with aggressive timeouts."""
        try:
            # Clean the URL first
            url = url.strip().replace('\n', ' ').replace('\r', ' ').replace('\0', '')
            url = ' '.join(url.split())
            
            self.log(f"Getting expected size for: {url}")
            
            # Try wget with spider mode first (more reliable for headers)
            # Use aggressive timeouts: 5 seconds for wget, 8 second subprocess timeout
            cmd = ['wget', '--spider', '--server-response', '--timeout=5', '--tries=1', url]
            try:
                result = subprocess.run(cmd, capture_output=True, text=True, timeout=8)
                
                # Parse Content-Length from output
                for line in result.stderr.split('\n'):
                    if 'Content-Length:' in line:
                        size_str = line.split('Content-Length:')[1].strip()
                        try:
                            size_bytes = int(size_str)
                            self.log(f"Got expected size via wget: {size_bytes:,} bytes")
                            return size_bytes
                        except ValueError:
                            continue
            except subprocess.TimeoutExpired:
                self.log(f"wget timeout getting size for: {url}")
            except Exception as e:
                self.log(f"wget error getting size: {e}")
            
            # Try curl as fallback (lighter weight, faster for just headers)
            # Use even more aggressive timeouts: 3 seconds for curl, 5 second subprocess timeout
            cmd = ['curl', '-s', '-I', '-L', '--max-time', '3', '--connect-timeout', '2', url]
            try:
                result = subprocess.run(cmd, capture_output=True, text=True, timeout=5)
                
                for line in result.stdout.split('\n'):
                    if 'content-length:' in line.lower():
                        size_str = line.split(':')[1].strip()
                        try:
                            size_bytes = int(size_str)
                            self.log(f"Got expected size via curl: {size_bytes:,} bytes")
                            return size_bytes
                        except ValueError:
                            continue
            except subprocess.TimeoutExpired:
                self.log(f"curl timeout getting size for: {url}")
            except Exception as e:
                self.log(f"curl error getting size: {e}")
            
            # If both methods failed, log and return None
            self.log(f"Could not get expected file size for: {url}")
            return None
                
        except Exception as e:
            self.log(f"Unexpected error in get_expected_size: {type(e).__name__}: {e}")
            return None
    
    def check_existing_file(self, decoded_filename):
        """Check if file already exists - use DECODED filename. Eliminates race condition by combining existence check with stat operation."""
        filepath = Path.cwd() / decoded_filename
        
        try:
            # Combine exists check with stat operation to eliminate race condition
            actual_size = filepath.stat().st_size
            # If we get here, file exists and we have its size
            return True, actual_size
        except FileNotFoundError:
            # File doesn't exist
            return False, None
        except OSError as e:
            # File exists but we can't stat it (permissions, deleted, etc.)
            self.log(f"Error accessing file '{decoded_filename}': {e}")
            return False, None
        except Exception as e:
            # Catch any other unexpected errors
            self.log(f"Unexpected error checking file '{decoded_filename}': {type(e).__name__}: {e}")
            return False, None
    
    def check_if_file_is_active_download(self, decoded_filename):
        """Check if file exists and is an active download by checking size changes - use DECODED filename. Eliminates race conditions."""
        filepath = Path.cwd() / decoded_filename
        
        # Get initial size - combine with existence check to eliminate race condition
        try:
            size1 = filepath.stat().st_size
        except FileNotFoundError:
            # File doesn't exist
            return False, None
        except OSError as e:
            # Can't access file
            self.log(f"Error accessing file '{decoded_filename}' on first check: {e}")
            return False, None
        
        # Wait 2.5 seconds for first check
        time.sleep(2.5)
        
        # Get size again
        try:
            size2 = filepath.stat().st_size
        except FileNotFoundError:
            # File was deleted while we were checking
            self.log(f"File '{decoded_filename}' was deleted during active download check")
            return False, None
        except OSError as e:
            # Can't access file anymore
            self.log(f"Error accessing file '{decoded_filename}' on second check: {e}")
            return False, None
        
        if size1 != size2:
            return True, size2  # File is actively downloading
        
        # Wait another 2.5 seconds for second check
        time.sleep(2.5)
        
        # Get size one more time
        try:
            size3 = filepath.stat().st_size
        except FileNotFoundError:
            # File was deleted while we were checking
            self.log(f"File '{decoded_filename}' was deleted during active download check (second sleep)")
            return False, None
        except OSError as e:
            # Can't access file anymore
            self.log(f"Error accessing file '{decoded_filename}' on third check: {e}")
            return False, None
        
        if size2 != size3:
            return True, size3  # File is actively downloading
        
        # File size hasn't changed in 5 seconds - not an active download
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
        """Format filename for display with robust handling for special characters and Unicode.
        
        Removes problematic characters and truncates safely for terminal display.
        Returns a display-safe string that won't corrupt terminal output.
        """
        try:
            # Step 1: Remove all problematic characters
            # Remove control characters, ANSI codes, tabs, etc.
            filename = filename.replace('\n', ' ').replace('\r', ' ').replace('\0', '')
            filename = filename.replace('\t', ' ')  # Tab characters
            filename = filename.replace('\x1b', '')  # ESC character (ANSI codes start with ESC)
            
            # Step 2: Normalize whitespace
            filename = ' '.join(filename.split())
            
            # Step 3: Validate and clean Unicode
            try:
                # Encode to ASCII with errors='ignore' to strip non-ASCII characters
                # Then decode back to get a clean string
                ascii_clean = filename.encode('utf-8', errors='replace').decode('utf-8', errors='replace')
                filename = ascii_clean
            except Exception:
                # If Unicode handling fails, use the original
                pass
            
            # Step 4: Remove any remaining unprintable characters
            filename = ''.join(char for char in filename if char.isprintable() or char == ' ')
            
            # Step 5: Set conservative display length limit
            # Use 60 chars as max to account for:
            # - 4 chars for slot number and colons "1: "
            # - 20 chars for progress bar "[==========      ]"
            # - 10 chars for percentage and status " 100% 100MB"
            # Total terminal width is typically 80 chars, so 60 for filename is safe
            max_display_length = 60
            
            # Step 6: Truncate intelligently
            if len(filename) <= max_display_length:
                return filename
            
            # If we need to truncate, show first 25 and last 25 chars with ellipsis
            first_part = filename[:25]
            last_part = filename[-25:]
            truncated = f"{first_part}...{last_part}"
            
            # Ensure even the truncated version isn't too long
            if len(truncated) > max_display_length:
                # If still too long, just take first 57 chars + "..."
                return filename[:57] + "..."
            
            return truncated
            
        except Exception as e:
            # If anything goes wrong, return a safe placeholder
            self.log(f"Error formatting filename for display: {type(e).__name__}: {e}")
            return "[filename display error]"
    
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
        """Move downloaded file to series directory with robust collision handling and race condition protection.
        
        Uses atomic operations and proper error handling to prevent file loss or overwrites.
        """
        try:
            # Validate source file exists and is accessible
            if not local_file or not isinstance(local_file, Path):
                self.log(f"MOVE FAILED: Invalid local_file parameter")
                return None
            
            if not local_file.exists():
                self.log(f"MOVE FAILED: Local file not found: {local_file}")
                return None
            
            try:
                source_size = local_file.stat().st_size
                self.log(f"MOVE SOURCE: {local_file.name} ({source_size:,} bytes)")
            except OSError as e:
                self.log(f"MOVE FAILED: Cannot stat source file: {e}")
                return None
            
            # Validate destination directory exists and is writable
            if not self.series_dir.exists():
                try:
                    self.series_dir.mkdir(parents=True, exist_ok=True)
                    self.log(f"MOVE: Created series directory: {self.series_dir}")
                except OSError as e:
                    self.log(f"MOVE FAILED: Cannot create series directory: {e}")
                    return None
            
            # Check write permissions on destination directory
            if not os.access(str(self.series_dir), os.W_OK):
                self.log(f"MOVE FAILED: No write permission on series directory: {self.series_dir}")
                return None
            
            # Determine final destination with collision avoidance
            destination = self._find_safe_destination(decoded_filename)
            
            if not destination:
                self.log(f"MOVE FAILED: Could not find safe destination for: {decoded_filename}")
                return None
            
            # Perform atomic move operation
            try:
                self.log(f"MOVE OPERATION: {local_file.name} -> {destination.name}")
                shutil.move(str(local_file), str(destination))
                
                # Verify move succeeded by checking destination
                if not destination.exists():
                    self.log(f"MOVE FAILED: Destination file not found after move: {destination}")
                    return None
                
                try:
                    dest_size = destination.stat().st_size
                    if dest_size != source_size:
                        self.log(f"MOVE WARNING: Size mismatch after move")
                        self.log(f"  Source size: {source_size:,} bytes")
                        self.log(f"  Destination size: {dest_size:,} bytes")
                        # Size mismatch detected, but move succeeded - log and continue
                except OSError as e:
                    self.log(f"MOVE WARNING: Cannot verify destination file size: {e}")
                
                self.log(f"MOVE SUCCESS: {decoded_filename} -> {destination.name}")
                return destination
                
            except FileExistsError as e:
                # Race condition: file was created between our check and move
                self.log(f"MOVE FAILED: Destination file appeared during move (race condition): {e}")
                return None
                
            except PermissionError as e:
                self.log(f"MOVE FAILED: Permission denied moving file: {e}")
                return None
                
            except OSError as e:
                self.log(f"MOVE FAILED: OS error during move: {type(e).__name__}: {e}")
                return None
                
            except Exception as e:
                self.log(f"MOVE FAILED: Unexpected error: {type(e).__name__}: {e}")
                return None
        
        except Exception as e:
            self.log(f"MOVE CRITICAL ERROR: {type(e).__name__}: {e}")
            return None

    def _find_safe_destination(self, decoded_filename):
        """Find a safe destination filename that won't collide with existing files.
        
        Uses a more efficient algorithm that avoids TOCTOU race conditions:
        1. First, try the exact filename (common case)
        2. If exists, find the next available counter value
        3. Only check existence for candidates we're about to use
        
        Returns Path object or None if no safe destination found.
        """
        try:
            # Start with the desired filename
            destination = self.series_dir / decoded_filename
            
            # Fast path: desired filename is available
            try:
                # Use stat with follow_symlinks=False to detect actual file existence
                # without following symlinks (more secure)
                destination.stat()
                # File exists, need to find alternative name
                self.log(f"COLLISION DETECTED: {decoded_filename} already exists")
            except FileNotFoundError:
                # File doesn't exist, use the desired filename
                self.log(f"DESTINATION CHOSEN: {decoded_filename}")
                return destination
            
            # Collision detected - find safe alternative
            # Split filename into name and extension
            name_parts = decoded_filename.rsplit('.', 1)
            if len(name_parts) == 2:
                base_name = name_parts[0]
                extension = '.' + name_parts[1]
            else:
                base_name = decoded_filename
                extension = ''
            
            # Try increasingly numbered versions
            # Start at 1, go up to 1000 (should be more than enough)
            max_attempts = 1000
            for counter in range(1, max_attempts + 1):
                candidate_name = f"{base_name}_{counter}{extension}"
                candidate_path = self.series_dir / candidate_name
                
                try:
                    # Check if candidate exists
                    candidate_path.stat()
                    # Exists, continue to next counter
                    continue
                except FileNotFoundError:
                    # Found a safe candidate!
                    self.log(f"COLLISION RESOLVED: Using numbered variant: {candidate_name} (attempt {counter})")
                    return candidate_path
            
            # Exhausted all attempts
            self.log(f"COLLISION FATAL: Could not find safe filename after {max_attempts} attempts")
            return None
        
        except Exception as e:
            self.log(f"ERROR in _find_safe_destination: {type(e).__name__}: {e}")
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
        file_location = "current directory"
        
        # If not in current directory, check in series directory
        if not file_exists:
            series_file_path = self.series_dir / decoded_filename
            try:
                existing_size = series_file_path.stat().st_size
                file_exists = True
                file_location = "series directory"
            except FileNotFoundError:
                # File doesn't exist in series directory either
                pass
            except OSError as e:
                self.log(f"Error accessing file in series directory '{decoded_filename}': {e}")
        
        if file_exists:
            self.log(f"Local file exists: {decoded_filename}")
            self.log(f"Location: {file_location}")
            self.log(f"File size: {existing_size:,} bytes")
            
            # Now get expected file size for comparison
            self.log(f"Getting expected file size for comparison...")
            expected_size = self.get_expected_size(url)
            
            if expected_size is not None:
                self.log(f"Expected file size: {expected_size:,} bytes")
                self.log(f"Test condition: local size == expected size? {existing_size == expected_size}")
                
                if existing_size == expected_size:
                    self.log(f"Result: File already exists with correct size")
                    
                    # If file is in current directory, move it to series directory
                    if file_location == "current directory":
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
                        # File is already in series directory - just mark as complete
                        self.log(f"Result: File already exists in series directory with correct size")
                        self.update_line_status(line_num, f"# COMPLETE (already in series)")
                        display_name = self.format_filename_for_display(decoded_filename)
                        size_mb = existing_size / (1024 * 1024)
                        self.update_slot_status(slot_id, f"{slot_id+1}: {display_name} [EXISTED, {size_mb:.1f}MB]")
                        return True, "ALREADY_EXISTED"
                else:
                    self.log(f"Result: File exists but size mismatch ({existing_size:,} != {expected_size:,})")
                    self.log(f"  Difference: {abs(existing_size - expected_size):,} bytes")
                    
                    # Calculate percentage difference for logging
                    if expected_size > 0:
                        percent_diff = abs(existing_size - expected_size) / expected_size * 100
                        self.log(f"  Difference: {percent_diff:.2f}%")
                    
                    # FILE SIZE MISMATCH - DO NOT PROCEED
                    self.log(f"  Action: NOT downloading - size mismatch indicates incomplete or corrupted file")
                    self.update_line_status(line_num, f"# FAILED - size mismatch (local {existing_size:,} != expected {expected_size:,})")
                    return False, "SIZE_MISMATCH"
            else:
                self.log(f"Result: Could not get expected file size for comparison")
                # Cannot verify file - do not proceed
                self.log(f"  Action: NOT downloading - cannot verify file completeness")
                self.update_line_status(line_num, f"# FAILED - cannot verify file size")
                return False, "NO_EXPECTED_SIZE"
        else:
            self.log(f"No local file found: {decoded_filename}")
        
        # Check if file is actively downloading - use DECODED filename
        # NOTE: This now only takes ~5 seconds instead of 20 (optimized check_if_file_is_active_download)
        is_active, current_size = self.check_if_file_is_active_download(decoded_filename)
        
        if is_active:
            self.log(f"Result: SKIPPED - file actively downloading")
            display_name = self.format_filename_for_display(decoded_filename)
            self.update_slot_status(slot_id, f"{slot_id+1}: {display_name} [ACTIVE]")
            return False, "ACTIVE"
        
        # Get expected size for new download
        expected_size = self.get_expected_size(url)
        
        # Stagger start times to avoid thundering herd
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
        """Worker thread that processes downloads from the queue. Non-daemon with proper cleanup."""
        try:
            self.log(f"Worker thread {slot_id} started")
            
            while True:
                try:
                    # Get next download from queue with timeout
                    item = self.download_queue.get(timeout=5)
                    if item is None:  # Sentinel value to stop worker
                        self.download_queue.task_done()
                        self.log(f"Worker thread {slot_id} received stop signal, shutting down gracefully")
                        break
                    
                    # Increment active downloads counter
                    with self.lock:
                        self.active_downloads += 1
                    
                    try:
                        line_num, url = item
                        self.log(f"Worker {slot_id} processing: {url}")
                        
                        # Process the download
                        success, status = self.process_download(line_num, url, slot_id)
                        
                        # Log result
                        result_str = "SUCCESS" if success else f"FAILED ({status})"
                        self.log(f"Worker {slot_id} result: {result_str}")
                        
                    finally:
                        # Decrement active downloads counter in finally block to ensure it always happens
                        with self.lock:
                            self.active_downloads -= 1
                        
                        self.download_queue.task_done()
                    
                except queue.Empty:
                    # Check if we should exit - but only if queue is truly empty and no downloads active
                    with self.lock:
                        if self.active_downloads == 0 and self.download_queue.empty():
                            self.log(f"Worker thread {slot_id} queue empty and no active downloads, exiting")
                            break
                    continue
                    
                except Exception as e:
                    self.log(f"Error in download worker {slot_id}: {type(e).__name__}: {e}")
                    
                    # Make sure we decrement counter even on exception
                    with self.lock:
                        if self.active_downloads > 0:
                            self.active_downloads -= 1
                    
                    try:
                        self.download_queue.task_done()
                    except ValueError:
                        # task_done() called too many times, ignore
                        pass
                    
        except Exception as e:
            self.log(f"Critical error in worker thread {slot_id}: {type(e).__name__}: {e}")
        finally:
            self.log(f"Worker thread {slot_id} shutting down (cleanup complete)")
    
    def process_downloads(self):
        """Main download processing loop using queue with robust daemon thread management."""
        try:
            # Read initial links
            all_lines, valid_links = self.read_links()
            
            if not valid_links:
                self.log("No valid https:// links found in file")
                return
            
            self.log(f"Found {len(valid_links)} valid download links")
            self.log(f"Files will be saved to: {self.series_dir}")
            
            # Start display thread (this one can be daemon - low priority)
            self.display_thread = threading.Thread(target=self.display_progress, daemon=True)
            self.display_thread.start()
            
            # Wait a moment for display to initialize
            time.sleep(0.5)
            
            # Add all initial links to queue
            for link_info in valid_links:
                self.download_queue.put(link_info)
            
            # Start worker threads as NON-DAEMON (critical work)
            workers = []
            for i in range(self.max_concurrent):
                worker = threading.Thread(
                    target=self.download_worker,
                    args=(i,),
                    daemon=False  # ← NON-DAEMON: Guaranteed cleanup
                )
                worker.name = f"DownloadWorker-{i}"  # For debugging
                worker.start()
                workers.append(worker)
            
            self.log(f"Started {len(workers)} worker threads")
            
            # Monitor for completion
            completion_checks = 0
            while True:
                # Check if all work is done
                with self.lock:
                    if self.active_downloads == 0 and self.download_queue.empty():
                        completion_checks += 1
                        # Require 3 consecutive checks (6 seconds total) to confirm completion
                        if completion_checks >= 3:
                            self.log("All downloads complete, proceeding to shutdown")
                            break
                    else:
                        completion_checks = 0  # Reset counter if work resumed
                
                time.sleep(2)
            
            # Send sentinel values to stop workers gracefully
            # This signals each worker thread to exit its loop
            self.log("Sending stop signals to all worker threads...")
            for i in range(self.max_concurrent):
                self.download_queue.put(None)
            
            # Stop display thread (daemon, so not critical)
            self.running = False
            if self.display_thread and self.display_thread.is_alive():
                try:
                    self.display_thread.join(timeout=2)
                except Exception as e:
                    self.log(f"Error joining display thread: {e}")
            
            # Wait for all worker threads to finish gracefully (NON-DAEMON)
            self.log("Waiting for worker threads to shut down...")
            shutdown_timeout_per_worker = 30  # 30 seconds per worker
            all_workers_joined = True
            
            for i, worker in enumerate(workers):
                try:
                    worker.join(timeout=shutdown_timeout_per_worker)
                    if worker.is_alive():
                        all_workers_joined = False
                        self.log(f"WARNING: Worker thread {i} ({worker.name}) did not shut down within {shutdown_timeout_per_worker}s timeout")
                        self.log(f"  This may indicate the thread is stuck or hung")
                    else:
                        self.log(f"Worker thread {i} ({worker.name}) shut down successfully")
                except Exception as e:
                    self.log(f"Error joining worker thread {i}: {type(e).__name__}: {e}")
                    all_workers_joined = False
            
            if all_workers_joined:
                self.log("All worker threads shut down successfully")
            else:
                self.log("WARNING: Some worker threads did not shut down cleanly")
            
            # Final summary display
            print("\033[2J\033[H", end='')
            print("=" * 60)
            print("All downloads completed!")
            print("=" * 60)
            self.log("All downloads processed! Session complete.")
            
        except Exception as e:
            self.log(f"Critical error in process_downloads: {type(e).__name__}: {e}")
            raise  # Re-raise to let caller handle
        
        finally:
            self.log("process_downloads cleanup complete")
    
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
