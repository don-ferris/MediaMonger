# MediaMonger

_Internet down? Can’t find anything good to watch on all those streaming services? Oversubscribed and overpaying and still not getting the content you want?_

**MediaMonger** is your personal, offline media library — an all‑in‑one, plug‑and‑play media‑server‑in‑a‑(virtual)‑box. Watch whatever you want, whenever you want, with a system that automatically downloads, processes, and organizes your content into a polished local library.

---

## Features

### 🎬 Core Pipeline
- Automated acquisition of Real‑Debrid links
- Download management with retry logic and integrity checks
- File renaming and normalization
- Subtitle inspection and (optional) acquisition
- Optional HandBrake‑based reencoding:
  - Adjust video quality for smaller file sizes
  - Create AC3 audio track if missing 
  - Preserve advanced audio (TrueHD, DTS‑HD MA, Atmos, DTS:X)
  - Remove non‑English audio tracks (unless primary track in foreign‑language films)
  - Retain English + forced subtitles, strip others
- Resolution‑based organization (4K / 1080p / 720p / SD)
- ntfy notifications for completion of long-running processes (reencoding) and error resolution.

### 📺 Integrated Services
- **Jellyfin** – self‑hosted media server for playback
- **DebridMediaManager (DMM)** – torrent selection and casting
- **MediaMonger Web UI (Flask)** – unified control center
- **Setup Wizard & Tutorial** – get running in minutes

### 🖥️ MediaMonger Web UI
A fixed‑header toolbar with iframe‑based content panes:
1. **Search IMDB** – query IMDB directly, results load below
2. **Add to Library** – send IMDB ID to DMM, choose torrent, cast → watch locally
3. **Fast filter buttons** - for narrowing down DMM torrents to those that meet specific criteria
4. **2DL Notepad** – jot quick notes about titles to download
5. **My RD Links** – view your Real‑Debrid links page
6. **History** – browse download history
7. **DL Log** – monitor current/past download activity
8. **VPN Status** – map view with red/green dot showing your public IP location
9. **Settings** – configure MediaMonger, RealDebrid, and DMM
10. **Documentation** – access full MediaMonger docs

---

## Quick Start

1. Clone the repo:
   ```bash
   git clone https://github.com/yourusername/MediaMonger.git
   cd MediaMonger

2. Start the containerized stack:
    ```bash
    docker compose up -d
  • setup wizard runs automatically on first run

3. Follow the guided tutorial to configure:
  • RealDebrid account
  • Jellyfin instance
  • DebridMediaManager
  • MediaMonger settings

4. go to [your server IP]:2222 to open the MediaMonger web UI in your browser and begin building your offline library.
5. Go to [your server IP] to use Jellyfin and start watching movies from your library.


---

Roadmap

* Phase 1: Acquisition & Download Management
* Phase 2: Metadata Analysis & Renaming
* Phase 3: Subtitles, Reencoding & Library Placement
* Phase 4: Interactive Problem Resolution
* Phase 5: Containerization & Distribution polish


---

License

GPL-3.0 License. See LICENSE for details.

---

Credits

* Jellyfin – Open source media server
* DebridMediaManager – Torrent selection and casting
* HandBrake – Video transcoder
* ntfy – Notifications
