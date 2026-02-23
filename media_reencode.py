#!/usr/bin/env python3
"""
media_reencode.py - Deterministic, metadata-driven media processing script
Supports file discovery, OMDB metadata lookup, stream analysis, and reencoding
"""

import os
import sys
import re
import json
import subprocess
import shutil
import logging
import time
import datetime
from pathlib import Path
from urllib.parse import quote, unquote
from html import unescape
from typing import List, Dict, Tuple, Optional, Any
import requests
from dataclasses import dataclass, field
from enum import Enum

# ============================================================================
# Configuration and Constants
# ============================================================================

# Load environment variables from .env file
from dotenv import load_dotenv
load_dotenv()

# OMDB API
OMDB_API_KEY = os.getenv('OMDB_API_KEY', '')
if not OMDB_API_KEY:
    print("Error: OMDB_API_KEY not found in .env file")
    sys.exit(1)

# NTFY configuration
NTFY_TOPIC = os.getenv('NTFY_TOPIC', 'heyudrf')
NTFY_URL = f"https://ntfy.sh/{NTFY_TOPIC}"

# Base directories
MOVIES_BASE = Path('/mnt/Media/Movies')
RESOLUTION_DIRS = {
    '4K': MOVIES_BASE / '4K',
    '1080': MOVIES_BASE / '1080',
    '720': MOVIES_BASE / '720',
    'SD': MOVIES_BASE / 'SD'
}

# File extensions to consider
MEDIA_EXTENSIONS = {'.mkv', '.mp4', '.avi', '.mov', '.wmv', '.flv', '.webm', '.m4v'}

# ============================================================================
# Data Classes
# ============================================================================

class SpatialAudioConfidence(Enum):
    VERY_HIGH = "Very High"
    HIGH = "High"
    MEDIUM = "Medium"
    LOW = "Low"

class DynamicRange(Enum):
    SDR = "SDR"
    HDR10 = "HDR10"
    HDR10PLUS = "HDR10+"
    HLG = "HLG"
    DOLBY_VISION = "Dolby Vision"

class SubtitleType(Enum):
    FORCED = "Forced"
    CLOSED_CAPTIONS = "Closed Captions"
    SDH = "SDH"
    HEARING_IMPAIRED = "Hearing Impaired"
    DEFAULT = "Default"
    COMMENTARY = "Commentary"
    OTHER = "Other"

class StreamFlag(Enum):
    KEEP = "KEEP"
    REMOVE = "REMOVE"
    CREATE = "CREATE"
    REENCODE = "REENCODE"
    DOWNLOAD = "DOWNLOAD"

@dataclass
class AudioStream:
    index: int
    codec_name: str
    codec_long_name: str
    profile: str
    tags: Dict[str, str]
    channel_layout: str
    channels: int
    bit_rate: str
    language: str
    spatial_type: Optional[str] = None
    spatial_confidence: Optional[SpatialAudioConfidence] = None
    flag: StreamFlag = StreamFlag.KEEP
    selector: str = ""

@dataclass
class VideoStream:
    index: int
    codec_name: str
    codec_long_name: str
    width: int
    height: int
    bit_rate: str
    dynamic_range: DynamicRange = DynamicRange.SDR
    bit_depth: int = 8
    flag: StreamFlag = StreamFlag.KEEP
    selector: str = ""

@dataclass
class SubtitleStream:
    index: int
    codec_name: str
    codec_long_name: str
    language: str
    type: SubtitleType
    tags: Dict[str, str]
    flag: StreamFlag = StreamFlag.KEEP
    selector: str = ""

@dataclass
class MediaMetadata:
    filename: Path
    raw_title: str
    sanitized_title: str
    filename_year: Optional[int]
    omdb_title: str = ""
    omdb_year: int = 2044
    imdb_id: str = "ttUNKNOWN"
    original_language: str = "Unknown"
    video_streams: List[VideoStream] = field(default_factory=list)
    audio_streams: List[AudioStream] = field(default_factory=list)
    subtitle_streams: List[SubtitleStream] = field(default_factory=list)
    file_size_gb: float = 0.0

# ============================================================================
# File Discovery
# ============================================================================

def find_files(search_string: str, directory: Path, recursive: bool = False) -> List[Path]:
    """Find files containing search_string (case-insensitive)."""
    matches = []
    pattern = re.compile(re.escape(search_string), re.IGNORECASE)
    
    if recursive:
        search_func = directory.rglob
    else:
        search_func = directory.glob
    
    for path in search_func('*'):
        if path.is_file() and pattern.search(path.name) and path.suffix.lower() in MEDIA_EXTENSIONS:
            matches.append(path)
    
    return matches

def select_from_matches(matches: List[Path]) -> Optional[Path]:
    """Display menu and let user select from multiple matches."""
    if len(matches) == 0:
        return None
    
    if len(matches) == 1:
        print(f"\nFound: {matches[0]}")
        response = input("Use this file? [Y/N/Q]: ").strip().upper()
        if response == 'Y':
            return matches[0]
        elif response == 'N':
            return None
        elif response == 'Q':
            sys.exit(0)
        return None
    
    # Multiple matches
    print("\nMultiple matches found:")
    for i, match in enumerate(matches):
        print(f"[{chr(65+i)}] {match}")
    print("[Q] Quit")
    
    while True:
        choice = input("\nSelect: ").strip().upper()
        if choice == 'Q':
            sys.exit(0)
        if len(choice) == 1 and 'A' <= choice <= chr(65 + len(matches) - 1):
            return matches[ord(choice) - 65]
        print("Invalid choice. Please try again.")

def discover_file(search_string: str) -> Optional[Path]:
    """Main file discovery workflow."""
    # Step 1: Search current directory
    current_dir = Path.cwd()
    matches = find_files(search_string, current_dir, recursive=False)
    
    if matches:
        result = select_from_matches(matches)
        if result:
            return result
    
    # Step 2: Search recursively in Movies directory
    print(f"\nSearching recursively in {MOVIES_BASE}...")
    matches = find_files(search_string, MOVIES_BASE, recursive=True)
    
    if matches:
        result = select_from_matches(matches)
        if result:
            return result
    else:
        print("\nNo matching files found.")
    
    return None

# ============================================================================
# Title Extraction and Sanitization
# ============================================================================

def extract_year(filename: str) -> Optional[int]:
    """Extract 4-digit year between 1921-2055 from filename."""
    year_pattern = r'(19[2-9][0-9]|20[0-4][0-9]|205[0-5])'
    matches = re.findall(year_pattern, filename)
    if matches:
        return int(matches[0])
    return None

def extract_raw_title(filename: str, year: Optional[int]) -> str:
    """Extract everything before the year in filename."""
    if year:
        # Split at year, take everything before it
        parts = re.split(str(year), filename, maxsplit=1)
        if parts and parts[0]:
            return parts[0].strip()
    # If no year or split failed, return filename without extension
    return Path(filename).stem

def sanitize_title(raw_title: str) -> str:
    """Apply all sanitization transformations."""
    # 1. Replace _ and . with spaces
    title = raw_title.replace('_', ' ').replace('.', ' ')
    
    # 2. Decode URL-encoded characters
    title = unquote(title)
    
    # 3. Decode HTML entities
    title = unescape(title)
    
    # 4. Remove trailing ( if present
    title = title.rstrip('(')
    
    # 5. Strip leading/trailing whitespace
    title = title.strip()
    
    # 6. Collapse multiple spaces
    title = re.sub(r'\s+', ' ', title)
    
    return title

# ============================================================================
# OMDB Lookup
# ============================================================================

def query_omdb(title: str, year: Optional[int]) -> Dict[str, Any]:
    """Query OMDB API for movie metadata."""
    params = {
        'apikey': OMDB_API_KEY,
        't': title,
        'y': year if year else '',
        'r': 'json'
    }
    
    try:
        response = requests.get('http://www.omdbapi.com/', params=params, timeout=10)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        logger.error(f"OMDB query failed: {e}")
        return {}

def extract_omdb_fields(data: Dict) -> Tuple[str, int, str, str]:
    """Extract fields from OMDB response."""
    title = data.get('Title', '')
    year = int(data.get('Year', '2044').split('–')[0]) if data.get('Year') else 2044
    imdb_id = data.get('imdbID', 'ttUNKNOWN')
    
    # Extract first language
    language = data.get('Language', 'Unknown')
    if language and ',' in language:
        language = language.split(',')[0].strip()
    
    return title, year, imdb_id, language

def imdb_fallback_search(sanitized_title: str) -> List[Tuple[str, str]]:
    """Fallback to IMDb search when OMDB fails."""
    # Convert title for URL
    search_title = sanitized_title.lower().replace(' ', '%20')
    url = f"https://www.imdb.com/find/?q={search_title}"
    
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        html = response.text
        
        # Find IMDb IDs
        pattern = r'href="/tt([0-9]{7,8})/'
        matches = re.findall(pattern, html)
        unique_matches = []
        
        for match in matches[:3]:
            imdb_id = f"tt{match}"
            if imdb_id not in unique_matches:
                unique_matches.append(imdb_id)
        
        # For simplicity, return placeholder titles
        # In a full implementation, you would parse actual titles
        candidates = []
        for i, imdb_id in enumerate(unique_matches):
            candidates.append((imdb_id, f"Movie {i+1} (from IMDb search)"))
        
        return candidates
        
    except requests.RequestException as e:
        logger.error(f"IMDb fallback search failed: {e}")
        return []

def send_ntfy_notification(title: str, message: str, priority: str = "default"):
    """Send notification via ntfy."""
    try:
        requests.post(
            NTFY_URL,
            data=message.encode('utf-8'),
            headers={
                'Title': title.encode('utf-8'),
                'Priority': priority
            },
            timeout=5
        )
    except requests.RequestException as e:
        logger.error(f"Failed to send ntfy notification: {e}")

def handle_metadata_error(metadata: MediaMetadata):
    """Handle metadata resolution errors with IMDb fallback.
    
    Raises:
        KeyboardInterrupt: If user chooses to quit (allows main() to handle cleanup)
    """
    error_desc = "Metadata resolution error: "
    if not metadata.filename_year:
        error_desc += "No release year in filename. "
    if metadata.imdb_id == "ttUNKNOWN":
        error_desc += "No IMDb ID from OMDB."
    
    logger.warning(error_desc)
    
    # Perform IMDb search
    candidates = imdb_fallback_search(metadata.sanitized_title)
    
    # Build notification message
    message = f"{error_desc}\n\n"
    message += "IMDb Candidates:\n"
    message += "IMDb Link\tTitle\n"
    for imdb_id, title in candidates:
        message += f"{imdb_id}\t{title}\n"
    
    message += "\nPlease enter IMDb ID manually."
    
    # Send notification
    send_ntfy_notification("Media Processor Error", message, "high")
    
    # Prompt user
    print("\n" + "="*60)
    print("METADATA RESOLUTION ERROR")
    print("="*60)
    print(error_desc)
    print("\nIMDb Candidates found:")
    
    if candidates:
        for imdb_id, title in candidates:
            print(f"  {imdb_id}: {title}")
    else:
        print("  No candidates found from IMDb search")
    
    while True:
        user_input = input("\nEnter IMDb ID (e.g., tt0133093) or Q to quit: ").strip()
        
        if user_input.upper() == 'Q':
            logger.info("User cancelled metadata resolution")
            raise KeyboardInterrupt("User chose to quit metadata resolution")
        
        if re.match(r'^tt[0-9]{7,8}$', user_input):
            metadata.imdb_id = user_input
            logger.info(f"User provided IMDb ID: {user_input}")
            
            # Retry OMDB with IMDb ID
            params = {
                'apikey': OMDB_API_KEY,
                'i': user_input,
                'r': 'json'
            }
            try:
                response = requests.get('http://www.omdbapi.com/', params=params, timeout=10)
                if response.status_code == 200:
                    data = response.json()
                    if data.get('Response') == 'True':
                        metadata.omdb_title, metadata.omdb_year, _, metadata.original_language = extract_omdb_fields(data)
                        logger.info(
                            f"Successfully retrieved metadata: {metadata.omdb_title} ({metadata.omdb_year})"
                        )
                        print(f"Retrieved metadata: {metadata.omdb_title} ({metadata.omdb_year})")
                        return
                    else:
                        logger.warning(f"OMDB returned False for IMDb ID: {user_input}")
                else:
                    logger.warning(f"OMDB returned status {response.status_code}")
            except requests.RequestException as e:
                logger.error(f"Failed to query OMDB with IMDb ID {user_input}: {e}")
            
            print("Could not retrieve metadata with IMDb ID. Using defaults.")
            logger.warning(
                f"Could not retrieve full metadata for {user_input}. "
                f"Using defaults: title='{metadata.omdb_title}', year={metadata.omdb_year}"
            )
            return
        
        print("Invalid IMDb ID format. Must be 'tt' followed by 7-8 digits.")
        logger.debug(f"User entered invalid IMDb ID format: {user_input}")
        
# ============================================================================
# Media Analysis with ffprobe/MediaInfo
# ============================================================================

def run_ffprobe(filepath: Path) -> Dict[str, Any]:
    """Run ffprobe and return JSON output."""
    cmd = [
        'ffprobe',
        '-v', 'quiet',
        '-print_format', 'json',
        '-show_streams',
        '-show_format',
        str(filepath)
    ]
    
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        return json.loads(result.stdout)
    except (subprocess.CalledProcessError, json.JSONDecodeError) as e:
        logger.error(f"ffprobe failed: {e}")
        return {}

def detect_dynamic_range(stream: Dict) -> DynamicRange:
    """Detect HDR type from video stream."""
    tags = stream.get('tags', {})
    side_data = stream.get('side_data_list', [])
    
    # Check for Dolby Vision
    if any('dolby' in str(value).lower() for value in tags.values()):
        return DynamicRange.DOLBY_VISION
    
    # Check side data for HDR info
    for data in side_data:
        if data.get('side_data_type') == 'Mastering display metadata':
            return DynamicRange.HDR10
        if data.get('side_data_type') == 'Content light level metadata':
            return DynamicRange.HDR10PLUS
    
    # Check codec/pixel format
    pix_fmt = stream.get('pix_fmt', '')
    if '10le' in pix_fmt or '10be' in pix_fmt:
        return DynamicRange.HDR10
    
    return DynamicRange.SDR

def detect_spatial_audio(audio_stream: Dict, filepath: Path) -> Tuple[Optional[str], Optional[SpatialAudioConfidence]]:
    """Detect spatial audio formats with confidence scoring."""
    codec_name = audio_stream.get('codec_name', '')
    tags = audio_stream.get('tags', {})
    
    # Dolby Atmos detection
    atmos_patterns = ['atmos', 'dolby atmos', 'eac3 joc']
    dtsx_patterns = ['dts:x', 'dts x', 'dts-x']
    
    # Check tags first (High confidence)
    for tag_value in tags.values():
        tag_lower = str(tag_value).lower()
        
        # Atmos in tags
        if any(pattern in tag_lower for pattern in atmos_patterns):
            return 'Dolby Atmos', SpatialAudioConfidence.HIGH
        
        # DTS:X in tags
        if any(pattern in tag_lower for pattern in dtsx_patterns):
            return 'DTS:X', SpatialAudioConfidence.HIGH
    
    # Check with MediaInfo (Very High confidence)
    try:
        result = subprocess.run(
            ['mediainfo', str(filepath)],
            capture_output=True,
            text=True,
            timeout=10
        )
        output = result.stdout.lower()
        
        if 'atmos' in output:
            return 'Dolby Atmos', SpatialAudioConfidence.VERY_HIGH
        if 'dts:x' in output:
            return 'DTS:X', SpatialAudioConfidence.VERY_HIGH
    except (subprocess.TimeoutExpired, subprocess.CalledProcessError, FileNotFoundError):
        pass
    
    # Codec-based detection (Medium confidence)
    if codec_name == 'truehd':
        return 'Dolby Atmos', SpatialAudioConfidence.MEDIUM
    if codec_name == 'eac3':
        return 'Dolby Atmos', SpatialAudioConfidence.MEDIUM
    if codec_name == 'dts' and audio_stream.get('profile', '').lower() == 'dts_hd_ma':
        return 'DTS:X', SpatialAudioConfidence.MEDIUM
    
    # Check for spatial indicators in channel layout
    channels = audio_stream.get('channels', 0)
    if channels >= 8:
        return 'Other Spatial', SpatialAudioConfidence.LOW
    
    return None, None

def detect_subtitle_type(sub_stream: Dict) -> SubtitleType:
    """Determine subtitle type from stream metadata."""
    tags = sub_stream.get('tags', {})
    disposition = sub_stream.get('disposition', {})
    
    # Check tags for type indicators
    for key, value in tags.items():
        value_lower = str(value).lower()
        
        if 'forced' in value_lower or (key == 'forced' and value == '1'):
            return SubtitleType.FORCED
        if 'sdh' in value_lower:
            return SubtitleType.SDH
        if 'hearing' in value_lower or 'cc' in value_lower:
            return SubtitleType.CLOSED_CAPTIONS
        if 'commentary' in value_lower:
            return SubtitleType.COMMENTARY
    
    # Check disposition
    if disposition.get('forced', 0) == 1:
        return SubtitleType.FORCED
    if disposition.get('hearing_impaired', 0) == 1:
        return SubtitleType.HEARING_IMPAIRED
    if disposition.get('default', 0) == 1:
        return SubtitleType.DEFAULT
    
    return SubtitleType.OTHER

def analyze_media(filepath: Path) -> MediaMetadata:
    """Main media analysis function."""
    logger.info(f"Analyzing media file: {filepath}")
    
    # Get basic file info
    file_size = filepath.stat().st_size / (1024**3)  # GB
    
    # Run ffprobe
    probe_data = run_ffprobe(filepath)
    
    if not probe_data:
        raise ValueError("Failed to analyze media file with ffprobe")
    
    # Extract metadata from filename
    filename_year = extract_year(filepath.name)
    raw_title = extract_raw_title(filepath.name, filename_year)
    sanitized_title = sanitize_title(raw_title)
    
    metadata = MediaMetadata(
        filename=filepath,
        raw_title=raw_title,
        sanitized_title=sanitized_title,
        filename_year=filename_year,
        file_size_gb=file_size
    )
    
    # Query OMDB
    omdb_data = query_omdb(metadata.sanitized_title, metadata.filename_year)
    
    if omdb_data.get('Response') == 'True':
        metadata.omdb_title, metadata.omdb_year, metadata.imdb_id, metadata.original_language = extract_omdb_fields(omdb_data)
    else:
        metadata.omdb_year = metadata.filename_year if metadata.filename_year else 2044
    
    # Handle metadata errors
    if not metadata.filename_year or metadata.imdb_id == "ttUNKNOWN":
        handle_metadata_error(metadata)
    
    # Parse streams
    streams = probe_data.get('streams', [])
    
    for i, stream in enumerate(streams):
        stream_type = stream.get('codec_type', '')
        index = stream.get('index', i)
        
        if stream_type == 'video':
            video_stream = VideoStream(
                index=index,
                codec_name=stream.get('codec_name', 'unknown'),
                codec_long_name=stream.get('codec_long_name', 'Unknown'),
                width=int(stream.get('width', 0)),
                height=int(stream.get('height', 0)),
                bit_rate=stream.get('bit_rate', '0'),
                dynamic_range=detect_dynamic_range(stream),
                bit_depth=int(stream.get('bits_per_raw_sample', 8))
            )
            metadata.video_streams.append(video_stream)
            
        elif stream_type == 'audio':
            tags = stream.get('tags', {})
            language = tags.get('language', 'und')
            
            spatial_type, spatial_confidence = detect_spatial_audio(stream, filepath)
            
            audio_stream = AudioStream(
                index=index,
                codec_name=stream.get('codec_name', 'unknown'),
                codec_long_name=stream.get('codec_long_name', 'Unknown'),
                profile=stream.get('profile', ''),
                tags=tags,
                channel_layout=stream.get('channel_layout', 'unknown'),
                channels=int(stream.get('channels', 0)),
                bit_rate=stream.get('bit_rate', '0'),
                language=language,
                spatial_type=spatial_type,
                spatial_confidence=spatial_confidence
            )
            metadata.audio_streams.append(audio_stream)
            
        elif stream_type == 'subtitle':
            tags = stream.get('tags', {})
            language = tags.get('language', 'und')
            
            subtitle_stream = SubtitleStream(
                index=index,
                codec_name=stream.get('codec_name', 'unknown'),
                codec_long_name=stream.get('codec_long_name', 'Unknown'),
                language=language,
                type=detect_subtitle_type(stream),
                tags=tags
            )
            metadata.subtitle_streams.append(subtitle_stream)
    
    return metadata

# ============================================================================
# Stream Selection Rules
# ============================================================================

def select_video_streams(metadata: MediaMetadata) -> None:
    """Apply video selection rules."""
    if not metadata.video_streams:
        return
    
    # Sort by priority
    def video_priority(video: VideoStream) -> tuple:
        # Resolution score (higher is better)
        if video.height >= 2160:
            res_score = 4
        elif video.height >= 1080:
            res_score = 3
        elif video.height >= 720:
            res_score = 2
        else:
            res_score = 1
        
        # HDR/DV score
        hdr_score = 0
        if video.dynamic_range != DynamicRange.SDR:
            hdr_score = 1
        
        # Bitrate (lower is better for tie-breaking)
        try:
            bitrate = int(video.bit_rate) if video.bit_rate else 0
        except ValueError:
            bitrate = 0
        
        return (-res_score, -hdr_score, bitrate)
    
    # Sort and select
    metadata.video_streams.sort(key=video_priority)
    
    # Keep only the best one
    for i, video in enumerate(metadata.video_streams):
        if i == 0:
            video.flag = StreamFlag.KEEP
        else:
            video.flag = StreamFlag.REMOVE

def select_audio_streams(metadata: MediaMetadata) -> None:
    """Apply audio selection rules based on original language."""
    if not metadata.audio_streams:
        return
    
    # Group audio streams
    english_streams = []
    original_lang_streams = []
    other_streams = []
    
    for audio in metadata.audio_streams:
        if audio.language.lower().startswith('en'):
            english_streams.append(audio)
        elif audio.language.lower().startswith(metadata.original_language.lower()[:2]):
            original_lang_streams.append(audio)
        else:
            other_streams.append(audio)
    
    # Determine if this is an English original-language movie
    is_english_original = metadata.original_language.lower().startswith('en')
    
    # Apply rules
    if is_english_original:
        # English original movies
        # Keep one English AC-3 5.1/7.1 (CREATE if needed)
        # Keep one English spatial (Atmos/DTS:X)
        
        ac3_streams = [s for s in english_streams if s.codec_name == 'ac3' and s.channels >= 6]
        spatial_streams = [s for s in english_streams if s.spatial_type in ['Dolby Atmos', 'DTS:X']]
        
        # Mark spatial stream to keep
        if spatial_streams:
            spatial_streams[0].flag = StreamFlag.KEEP
            # Remove other spatial streams
            for s in spatial_streams[1:]:
                s.flag = StreamFlag.REMOVE
        
        # Mark AC-3 stream to keep or create
        if ac3_streams:
            ac3_streams[0].flag = StreamFlag.KEEP
            # Remove other AC-3 streams
            for s in ac3_streams[1:]:
                s.flag = StreamFlag.REMOVE
        else:
            # Need to create AC-3
            # Find a suitable source stream with 6+ channels
            source_stream = None
            for s in english_streams:
                if s.flag != StreamFlag.REMOVE and s.channels >= 6:
                    source_stream = s
                    break
            
            if source_stream:
                # Create a new AC-3 stream entry
                new_ac3 = AudioStream(
                    index=-1,  # Will be assigned later
                    codec_name='ac3',
                    codec_long_name='ATSC A/52B (AC-3, E-AC-3)',
                    profile='',
                    tags={'language': 'eng', 'title': 'Created AC-3 5.1'},
                    channel_layout='5.1',
                    channels=6,
                    bit_rate='640000',
                    language='eng',
                    flag=StreamFlag.CREATE
                )
                metadata.audio_streams.append(new_ac3)
            else:
                logger.warning(
                    "No suitable English audio stream with 6+ channels found for AC-3 creation. "
                    "Will keep existing English streams as-is."
                )
        
        # Remove unwanted streams
        for s in english_streams:
            if s.flag == StreamFlag.KEEP:
                continue
            if s.codec_name in ['aac', 'dts', 'flac', 'pcm']:
                s.flag = StreamFlag.REMOVE
            elif s.channels < 6:  # Mono/stereo
                s.flag = StreamFlag.REMOVE
        
    else:
        # Non-English original movies
        # Keep one original-language AC-3 (or stereo)
        # Keep one English AC-3
        
        # Find original language AC-3
        orig_ac3 = None
        for s in original_lang_streams:
            if s.codec_name == 'ac3':
                orig_ac3 = s
                break
        
        if orig_ac3:
            orig_ac3.flag = StreamFlag.KEEP
        else:
            # Keep best original language stream
            if original_lang_streams:
                original_lang_streams[0].flag = StreamFlag.KEEP
                logger.info(
                    f"No original language AC-3 found. Keeping best available: "
                    f"{original_lang_streams[0].codec_name} {original_lang_streams[0].language}"
                )
            else:
                logger.warning("No original language audio streams found.")
        
        # Find English AC-3
        eng_ac3 = None
        for s in english_streams:
            if s.codec_name == 'ac3':
                eng_ac3 = s
                break
        
        if eng_ac3:
            eng_ac3.flag = StreamFlag.KEEP
        else:
            # Create English AC-3 if needed
            source_stream = None
            for s in english_streams:
                if s.channels >= 2:
                    source_stream = s
                    break
            
            if source_stream:
                new_ac3 = AudioStream(
                    index=-1,
                    codec_name='ac3',
                    codec_long_name='ATSC A/52B (AC-3, E-AC-3)',
                    profile='',
                    tags={'language': 'eng', 'title': 'Created English AC-3'},
                    channel_layout='stereo' if source_stream.channels < 6 else '5.1',
                    channels=min(source_stream.channels, 6),
                    bit_rate='192000' if source_stream.channels < 6 else '640000',
                    language='eng',
                    flag=StreamFlag.CREATE
                )
                metadata.audio_streams.append(new_ac3)
            else:
                logger.warning(
                    "No English audio stream with 2+ channels found for AC-3 creation. "
                    "English soundtrack may not be available."
                )
    
    # Remove all other languages
    for s in other_streams:
        s.flag = StreamFlag.REMOVE

def select_subtitle_streams(metadata: MediaMetadata) -> None:
    """Apply subtitle selection rules."""
    if not metadata.subtitle_streams:
        return
    
    # Keep all English subtitles
    for sub in metadata.subtitle_streams:
        if sub.language.lower().startswith('en'):
            sub.flag = StreamFlag.KEEP
    
    # Keep all forced subtitles regardless of language
    for sub in metadata.subtitle_streams:
        if sub.type == SubtitleType.FORCED:
            sub.flag = StreamFlag.KEEP
    
    # Check if we have English CC/SDH
    has_english_cc = any(
        sub.flag == StreamFlag.KEEP and 
        sub.language.lower().startswith('en') and
        sub.type in [SubtitleType.CLOSED_CAPTIONS, SubtitleType.SDH, SubtitleType.HEARING_IMPAIRED]
        for sub in metadata.subtitle_streams
    )
    
    if not has_english_cc:
        # Need to download from OpenSubtitles
        new_sub = SubtitleStream(
            index=-1,
            codec_name='subrip',
            codec_long_name='SubRip subtitle',
            language='eng',
            type=SubtitleType.CLOSED_CAPTIONS,
            tags={'title': 'Downloaded from OpenSubtitles'},
            flag=StreamFlag.DOWNLOAD
        )
        metadata.subtitle_streams.append(new_sub)
    
    # Remove non-English non-forced subtitles
    for sub in metadata.subtitle_streams:
        if sub.flag == StreamFlag.KEEP:
            continue
        if not sub.language.lower().startswith('en') and sub.type != SubtitleType.FORCED:
            sub.flag = StreamFlag.REMOVE

# ============================================================================
# User Interface and Menu System
# ============================================================================

def assign_selectors(metadata: MediaMetadata):
    """Assign [A], [B], [C]... selectors to streams."""
    all_streams = []
    all_streams.extend(metadata.video_streams)
    all_streams.extend(metadata.audio_streams)
    all_streams.extend(metadata.subtitle_streams)
    
    # Filter out CREATE/DOWNLOAD streams (they don't get selectors)
    selectable_streams = [s for s in all_streams if s.flag not in [StreamFlag.CREATE, StreamFlag.DOWNLOAD]]
    
    for i, stream in enumerate(selectable_streams):
        stream.selector = chr(65 + i)

def display_metadata_report(metadata: MediaMetadata):
    """Display comprehensive metadata report."""
    print("\n" + "="*80)
    print("MEDIA METADATA REPORT")
    print("="*80)
    
    print(f"\nFile: {metadata.filename}")
    print(f"Size: {metadata.file_size_gb:.2f} GB")
    print(f"Title: {metadata.omdb_title} ({metadata.omdb_year})")
    print(f"IMDb ID: {metadata.imdb_id}")
    print(f"Original Language: {metadata.original_language}")
    
    print("\n" + "-"*80)
    print("VIDEO STREAMS")
    print("-"*80)
    for video in metadata.video_streams:
        dr = video.dynamic_range.value if hasattr(video.dynamic_range, 'value') else video.dynamic_range
        print(f"  Stream {video.index}: {video.codec_name} {video.width}x{video.height} {dr} {video.flag.value}")
    
    print("\n" + "-"*80)
    print("AUDIO STREAMS")
    print("-"*80)
    for audio in metadata.audio_streams:
        spatial = f" [{audio.spatial_type}]" if audio.spatial_type else ""
        conf = f" ({audio.spatial_confidence.value})" if audio.spatial_confidence else ""
        print(f"  Stream {audio.index}: {audio.codec_name} {audio.channel_layout} {audio.language}{spatial}{conf} {audio.flag.value}")
    
    print("\n" + "-"*80)
    print("SUBTITLE STREAMS")
    print("-"*80)
    for sub in metadata.subtitle_streams:
        print(f"  Stream {sub.index}: {sub.codec_name} {sub.language} {sub.type.value} {sub.flag.value}")
    
    print("\n" + "="*80)
    print("REENCODE RECOMMENDATIONS")
    print("="*80)
    
    # Summary display
    print("\nVideo:")
    for video in metadata.video_streams:
        print(f"  • {video.flag.value}: {video.codec_name} {video.width}x{video.height}")
    
    print("\nAudio:")
    for audio in metadata.audio_streams:
        if audio.flag in [StreamFlag.KEEP, StreamFlag.CREATE, StreamFlag.REENCODE]:
            print(f"  • {audio.flag.value}: {audio.codec_name} {audio.channel_layout} {audio.language}")
    
    print("\nSubtitles:")
    for sub in metadata.subtitle_streams:
        if sub.flag in [StreamFlag.KEEP, StreamFlag.DOWNLOAD]:
            print(f"  • {sub.flag.value}: {sub.language} {sub.type.value}")
    
    # Check for DOWNLOAD needed
    download_needed = any(sub.flag == StreamFlag.DOWNLOAD for sub in metadata.subtitle_streams)
    if download_needed:
        print("\n  DOWNLOAD: English CC/SDH subtitles from OpenSubtitles")

def build_action_menu(metadata: MediaMetadata) -> Dict[str, str]:
    """Build dynamic action menu based on conditions."""
    menu = {}
    
    # [V] - Reencode Video only
    if metadata.file_size_gb > 15:
        menu['V'] = "Reencode Video only"
    
    # [A] - Reencode Audio only
    audio_needs_work = any(a.flag in [StreamFlag.CREATE, StreamFlag.REMOVE] for a in metadata.audio_streams)
    if audio_needs_work and not (metadata.file_size_gb > 15):
        menu['A'] = "Reencode Audio only"
    
    # [B] - Reencode Both Audio and Video
    if metadata.file_size_gb > 15 and audio_needs_work:
        menu['B'] = "Reencode Both Audio and Video"
    
    # [C] - Customize kept/removed streams
    menu['C'] = "Customize kept/removed streams"
    
    # [R] - Rename/move file
    # Check if filename needs renaming
    new_filename = generate_new_filename(metadata)
    if new_filename != metadata.filename.name:
        menu['R'] = "Rename/move file"
    
    # [Q] - Quit
    menu['Q'] = "Quit"
    
    return menu

def display_action_menu(menu: Dict[str, str]):
    """Display action menu to user."""
    print("\n" + "="*80)
    print("ACTION MENU")
    print("="*80)
    
    for key, description in menu.items():
        print(f"[{key}] {description}")

def handle_customize_streams(metadata: MediaMetadata):
    """Handle Option C - Custom stream selection."""
    assign_selectors(metadata)
    
    print("\n" + "="*80)
    print("CUSTOM STREAM SELECTION")
    print("="*80)
    
    # Display streams with selectors
    print("\nAvailable streams:")
    
    all_streams = []
    all_streams.extend(metadata.video_streams)
    all_streams.extend(metadata.audio_streams)
    all_streams.extend(metadata.subtitle_streams)
    
    for stream in all_streams:
        if stream.selector:  # Only show streams with selectors
            if isinstance(stream, VideoStream):
                # Video stream
                print(f"  [{stream.selector}] Video: {stream.codec_name} {stream.width}x{stream.height}")
            elif isinstance(stream, AudioStream):
                # Audio stream
                print(f"  [{stream.selector}] Audio: {stream.codec_name} {stream.channel_layout} {stream.language}")
            elif isinstance(stream, SubtitleStream):
                # Subtitle stream
                print(f"  [{stream.selector}] Subtitle: {stream.language} {stream.type.value}")
    
    print("\nEnter letters of streams to KEEP (e.g., ACFH):")
    print("Streams not listed will be REMOVED")
    print("CREATE/DOWNLOAD streams will be preserved if selected")
    
    while True:
        choice = input("\nSelection: ").strip().upper()
        
        # Reset all flags
        for stream in all_streams:
            if stream.flag not in [StreamFlag.CREATE, StreamFlag.DOWNLOAD]:
                stream.flag = StreamFlag.REMOVE
        
        # Mark selected streams as KEEP
        for char in choice:
            for stream in all_streams:
                if stream.selector == char:
                    stream.flag = StreamFlag.KEEP
        
        # Validate selection using isinstance for type-safe checking
        has_video = any(
            isinstance(s, VideoStream) and s.flag == StreamFlag.KEEP 
            for s in all_streams
        )
        has_audio = any(
            isinstance(s, AudioStream) and s.flag == StreamFlag.KEEP 
            for s in all_streams
        )
        has_english_sub = any(
            isinstance(s, SubtitleStream) and
            s.flag == StreamFlag.KEEP and 
            s.language.lower().startswith('en')
            for s in all_streams
        )
        has_cc_sdh = any(
            isinstance(s, SubtitleStream) and
            s.flag == StreamFlag.KEEP and
            s.type in [SubtitleType.CLOSED_CAPTIONS, SubtitleType.SDH, SubtitleType.HEARING_IMPAIRED]
            for s in all_streams
        )
        
        # Validate and provide clear error messages
        if not has_video:
            print("Error: Must keep at least one video stream")
            continue
        if not has_audio:
            print("Error: Must keep at least one audio stream")
            continue
        if not has_english_sub:
            print("Error: Must keep at least one English subtitle")
            continue
        if not has_cc_sdh:
            print("Error: Must keep at least one CC/SDH (closed captions/hearing impaired) subtitle")
            continue
        
        logger.info(
            f"Custom stream selection complete: "
            f"{sum(1 for s in all_streams if isinstance(s, VideoStream) and s.flag == StreamFlag.KEEP)} video, "
            f"{sum(1 for s in all_streams if isinstance(s, AudioStream) and s.flag == StreamFlag.KEEP)} audio, "
            f"{sum(1 for s in all_streams if isinstance(s, SubtitleStream) and s.flag == StreamFlag.KEEP)} subtitle streams selected"
        )
        break
# ============================================================================
# Reencoding and File Operations
# ============================================================================

def generate_new_filename(metadata: MediaMetadata) -> str:
    """Generate new filename based on metadata."""
    # Extract resolution from video stream
    resolution = "Unknown"
    if metadata.video_streams:
        height = metadata.video_streams[0].height
        if height >= 2160:
            resolution = "2160p"
        elif height >= 1080:
            resolution = "1080p"
        elif height >= 720:
            resolution = "720p"
        else:
            resolution = "SD"
    
    # Clean title for filename
    clean_title = re.sub(r'[^\w\s-]', '', metadata.omdb_title)
    clean_title = re.sub(r'[-\s]+', '.', clean_title).strip('.-')
    
    # Format: [MT] ([RY]).[II].[REZ].[EXT]
    ext = metadata.filename.suffix
    new_name = f"{clean_title}.({metadata.omdb_year}).{metadata.imdb_id}.{resolution}{ext}"
    
    return new_name

def build_ffmpeg_command(metadata: MediaMetadata, option: str) -> str:
    """Build ffmpeg command based on selected option."""
    cmd = ["ffmpeg", "-i", str(metadata.filename), "-map_metadata", "0"]
    
    # Map video stream
    for video in metadata.video_streams:
        if video.flag == StreamFlag.KEEP:
            cmd.extend(["-map", f"0:{video.index}"])
            if option in ['V', 'B']:
                # Add video encoding parameters
                cmd.extend([
                    "-c:v", "libx265",
                    "-preset", "medium",
                    "-crf", "23",
                    "-tag:v", "hvc1"
                ])
            else:
                cmd.extend(["-c:v", "copy"])
    
    # Map audio streams
    for audio in metadata.audio_streams:
        if audio.flag == StreamFlag.KEEP:
            cmd.extend(["-map", f"0:{audio.index}"])
            cmd.extend(["-c:a", "copy"])
        elif audio.flag == StreamFlag.CREATE:
            # Need to create AC-3 from existing stream
            # Find source stream with matching language
            source_stream = None
            for a in metadata.audio_streams:
                if a.flag == StreamFlag.KEEP and a.language == audio.language:
                    source_stream = a
                    break
            
            if source_stream:
                cmd.extend(["-map", f"0:{source_stream.index}"])
                cmd.extend([
                    "-c:a", "ac3",
                    "-b:a", audio.bit_rate,
                    "-ac", str(audio.channels)
                ])
            else:
                logger.warning(
                    f"No source stream found for creating AC-3 audio track "
                    f"(language: {audio.language}). Skipping creation."
                )
    
    # Map subtitle streams
    for sub in metadata.subtitle_streams:
        if sub.flag == StreamFlag.KEEP:
            cmd.extend(["-map", f"0:{sub.index}"])
            cmd.extend(["-c:s", "copy"])
    
    # Add metadata
    cmd.extend([
        "-metadata", f"title={metadata.omdb_title}",
        "-metadata", f"year={metadata.omdb_year}",
        "-metadata", f"comment=Processed by media_reencode.py"
    ])
    
    # Output file
    output_file = metadata.filename.parent / "temp_reencode.mkv"
    cmd.append(str(output_file))
    
    return " ".join(cmd)
def reencode_media(metadata: MediaMetadata, option: str):
    """Execute reencoding process."""
    cmd = build_ffmpeg_command(metadata, option)
    
    print("\n" + "="*80)
    print("REENCODE COMMAND")
    print("="*80)
    print(f"\nCommand:\n{cmd}")
    
    print("\nParameters:")
    print("  -map_metadata 0          : Copy all metadata from input")
    print("  -map 0:n                 : Select stream n from input")
    print("  -c:v libx265             : Encode video with H.265/HEVC")
    print("  -preset medium           : Encoding speed/quality tradeoff")
    print("  -crf 23                  : Constant Rate Factor (quality)")
    print("  -c:a copy                : Copy audio without reencoding")
    print("  -c:s copy                : Copy subtitles without reencoding")
    
    response = input("\nRun command? [Y/N/Q]: ").strip().upper()
    if response != 'Y':
        return
    
    # Send start notification
    start_time = datetime.datetime.now()
    send_ntfy_notification(
        "Reencode Started",
        f"Reencode started for {metadata.filename.name} at {start_time.strftime('%Y-%m-%d %H:%M:%S')}",
        "default"
    )
    
    print(f"\nStarting reencode at {start_time}")
    print("This may take a while...")
    
    try:
        # Run the command
        process = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        
        end_time = datetime.datetime.now()
        duration = end_time - start_time
        
        if process.returncode == 0:
            print(f"\nReencode completed successfully!")
            print(f"Started: {start_time}")
            print(f"Ended: {end_time}")
            print(f"Duration: {duration}")
            
            # Send success notification
            send_ntfy_notification(
                "Reencode Completed",
                f"Reencode completed for {metadata.filename.name}\n"
                f"Duration: {duration}\n"
                f"File saved as: temp_reencode.mkv",
                "default"
            )
            
            # Replace original with new file
            backup_file = metadata.filename.with_suffix('.bak' + metadata.filename.suffix)
            shutil.move(metadata.filename, backup_file)
            shutil.move(metadata.filename.parent / "temp_reencode.mkv", metadata.filename)
            
            print(f"\nOriginal file backed up as: {backup_file}")
            print(f"New file saved as: {metadata.filename}")
            
        else:
            print(f"\nReencode failed with return code {process.returncode}")
            print(f"Error output:\n{process.stderr}")
            
            send_ntfy_notification(
                "Reencode Failed",
                f"Reencode failed for {metadata.filename.name}\n"
                f"Error: {process.stderr[:200]}...",
                "high"
            )
            
    except Exception as e:
        print(f"\nReencode failed: {e}")
        send_ntfy_notification(
            "Reencode Failed",
            f"Reencode failed for {metadata.filename.name}\nError: {e}",
            "high"
        )

def rename_and_move_file(metadata: MediaMetadata):
    """Handle Option R - Rename and move file."""
    new_name = generate_new_filename(metadata)
    new_path = metadata.filename.parent / new_name
    
    print(f"\nCurrent filename: {metadata.filename.name}")
    print(f"New filename: {new_name}")
    
    # Determine destination directory based on resolution
    dest_dir = None
    if metadata.video_streams:
        height = metadata.video_streams[0].height
        if height >= 2160:
            dest_dir = RESOLUTION_DIRS['4K']
        elif height >= 1080:
            dest_dir = RESOLUTION_DIRS['1080']
        elif height >= 720:
            dest_dir = RESOLUTION_DIRS['720']
        else:
            dest_dir = RESOLUTION_DIRS['SD']
    
    print(f"Destination directory: {dest_dir}")
    
    response = input("\nProceed with rename/move? [Y/N]: ").strip().upper()
    if response != 'Y':
        return
    
    try:
        # Rename file
        if new_name != metadata.filename.name:
            metadata.filename = metadata.filename.rename(new_path)
            print(f"Renamed to: {metadata.filename}")
        
        # Move to appropriate directory if needed
        if dest_dir and not metadata.filename.parent.samefile(dest_dir):
            dest_dir.mkdir(parents=True, exist_ok=True)
            new_location = dest_dir / metadata.filename.name
            shutil.move(metadata.filename, new_location)
            metadata.filename = new_location
            print(f"Moved to: {new_location}")
        else:
            print("File already in correct directory.")
            
    except Exception as e:
        print(f"Error during rename/move: {e}")
        send_ntfy_notification(
            "Rename/Move Failed",
            f"Failed to rename/move {metadata.filename}\nError: {e}",
            "high"
        )

# ============================================================================
# Logging System
# ============================================================================

def setup_logging():
    """Setup logging to both file and console."""
    global logger
    
    # Create logger
    logger = logging.getLogger('media_processor')
    logger.setLevel(logging.DEBUG)
    
    # Create formatters
    file_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    console_formatter = logging.Formatter('%(levelname)s: %(message)s')
    
    # File handler (main log)
    script_dir = Path(__file__).parent
    main_log = script_dir / 'media_processor.log'
    file_handler = logging.FileHandler(main_log, encoding='utf-8')
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(file_formatter)
    
    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(console_formatter)
    
    # Temp log handler
    temp_log = Path(f'/tmp/media_processor_{os.getpid()}.log')
    temp_handler = logging.FileHandler(temp_log, encoding='utf-8')
    temp_handler.setLevel(logging.DEBUG)
    temp_handler.setFormatter(file_formatter)
    
    # Add handlers
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    logger.addHandler(temp_handler)
    
    return temp_log

def cleanup_logging(temp_log: Path):
    """Prepend temp log to main log and clean up."""
    try:
        # Read temp log
        if temp_log.exists():
            with open(temp_log, 'r', encoding='utf-8') as f:
                temp_content = f.read()
            
            # Read main log
            script_dir = Path(__file__).parent
            main_log = script_dir / 'media_processor.log'
            main_content = ""
            if main_log.exists():
                with open(main_log, 'r', encoding='utf-8') as f:
                    main_content = f.read()
            
            # Write combined content
            with open(main_log, 'w', encoding='utf-8') as f:
                f.write(temp_content + main_content)
            
            # Delete temp log
            temp_log.unlink()
            
    except Exception as e:
        print(f"Warning: Failed to clean up logs: {e}")

# ============================================================================
# Main Function
# ============================================================================

def main():
    """Main function."""
    # Check arguments
    if len(sys.argv) != 2:
        print("Usage: media_reencode.py <search_string>")
        sys.exit(1)
    
    search_string = sys.argv[1]
    
    # Setup logging
    temp_log = setup_logging()
    
    try:
        # File discovery
        print(f"Searching for: {search_string}")
        filepath = discover_file(search_string)
        
        if not filepath:
            print("No file selected. Exiting.")
            sys.exit(0)
        
        print(f"\nSelected file: {filepath}")
        
        # Analyze media
        metadata = analyze_media(filepath)
        
        # Apply selection rules
        select_video_streams(metadata)
        select_audio_streams(metadata)
        select_subtitle_streams(metadata)
        
        # Display report
        display_metadata_report(metadata)
        
        # Main loop
        while True:
            # Build and display action menu
            menu = build_action_menu(metadata)
            display_action_menu(menu)
            
            choice = input("\nSelect option: ").strip().upper()
            
            if choice == 'Q':
                print("Exiting.")
                break
                
            elif choice == 'V' and 'V' in menu:
                reencode_media(metadata, 'V')
                
            elif choice == 'A' and 'A' in menu:
                reencode_media(metadata, 'A')
                
            elif choice == 'B' and 'B' in menu:
                reencode_media(metadata, 'B')
                
            elif choice == 'C' and 'C' in menu:
                handle_customize_streams(metadata)
                # Reapply rules based on custom selection
                select_audio_streams(metadata)  # Keep audio rules
                select_subtitle_streams(metadata)  # Keep subtitle rules
                display_metadata_report(metadata)
                
            elif choice == 'R' and 'R' in menu:
                rename_and_move_file(metadata)
                # Update menu since filename changed
                menu = build_action_menu(metadata)
                
            else:
                print("Invalid option. Please try again.")
        
    except KeyboardInterrupt:
        print("\n\nInterrupted by user.")
        send_ntfy_notification("Media Processor Interrupted", "Script was interrupted by user", "low")
        
    except Exception as e:
        logger.exception("Fatal error during processing")
        send_ntfy_notification(
            "Media Processor Error",
            f"Error during media processing:\n{str(e)}\n\nScript aborted. Manual intervention required.",
            "high"
        )
        print(f"\nFatal error: {e}")
        print("Check log for details.")
        
    finally:
        # Cleanup logging
        cleanup_logging(temp_log)

if __name__ == "__main__":
    main()
