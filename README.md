# x3-sync

Sends new books from a Calibre library to an Xteink X3 running CrossPoint
firmware. Each book is optimized for the X3 before upload, using the same
optimizer as CrossPoint's Calibre plugin:

- images are scaled to the X3's 528×792 screen and converted to grayscale JPEG
- oversized paragraphs and chapters are split, and embedded fonts are removed,
  so the firmware doesn't run out of memory
- covers, OPF and NCX are cleaned up

The service runs next to Calibre-Web-Automated. CWA ingests books and emails
the Kindle. x3-sync watches the library and pushes new EPUBs to the X3 the
next time the device is on the **File Transfer** screen.

## How it works

Every `POLL_INTERVAL` seconds the service:

1. Reads `metadata.db` read-only and lists the EPUB books that haven't been
   sent yet. Books are tracked by Calibre uuid, so renames and metadata edits
   don't trigger a resend.
2. Skips books modified less than `SETTLE_SECONDS` ago, so CWA can finish
   fetching metadata and converting.
3. If anything is queued, it looks for the X3. This uses CrossPoint's UDP
   discovery, unicast to `X3_HOST`, with a TCP probe of port 81 as fallback.
   The X3 only answers while it's in File Transfer mode.
4. Creates the book's folders on the X3 if needed (`{author}/{title}` by
   default, so `/Frank Herbert/Dune.epub`), optimizes the book, uploads it over
   CrossPoint's WebSocket protocol, and records it in `/config/state.json`.

Behaviour to know about:

- **First run.** With `SEED_EXISTING=true` (the default), every book already
  in the library is marked as sent, so only books added from then on go to
  the X3. To push the whole library instead, set it to `false` before the
  first start.
- **Interrupted transfers.** If the X3 leaves transfer mode mid-queue, the
  current book stays queued and the round stops. It resumes the next time the
  device appears.
- **Optimization errors.** If optimizing a book fails, the original file is
  sent, which is the plugin's behaviour too.
- **Existing files.** The CrossPoint firmware refuses to upload over a file
  that already exists. If the target file is already on the X3, the book is
  skipped and counted as sent; with `OVERWRITE_EXISTING=true` the old copy is
  deleted and the book uploaded again. A partial file left by a dropped
  transfer is always deleted before the retry.

## Setup on Unraid

### 1. Prepare the X3

- Flash CrossPoint on the X3.
- In your router, give the X3 a DHCP reservation. A fixed IP lets the
  container stay on the normal `bridge` network.

### 2. Build the image

From the Unraid terminal:

```sh
mkdir -p /mnt/user/appdata/x3-sync-src && cd /mnt/user/appdata/x3-sync-src
# copy this folder here, then:
docker build -t x3-sync:latest .
```

### 3. Create the container

- Copy `x3-sync.xml` to `/boot/config/plugins/dockerMan/templates-user/my-x3-sync.xml`.
- In Unraid, open **Docker → Add Container** and pick the **x3-sync** template.
- Set **Calibre library** to the same folder CWA mounts as `/calibre-library`,
  the one that contains `metadata.db`.
- Set **X3 IP address**.

Alternatively, use `docker-compose.yml` with the Compose Manager plugin.

### 4. Test it

- Add a book to CWA's ingest folder.
- Wait for `SETTLE_SECONDS`, which is 5 minutes by default.
- On the X3, open Settings → File Transfer.
- The container log should show `X3 found`, then the optimizer summary,
  then `sent`.

## Configuration

| Variable | Default | |
|---|---|---|
| `X3_HOST` | *(empty)* | Fixed IP of the X3. If empty, broadcast discovery is used, which needs `br0` or host networking |
| `X3_PORT` | `81` | CrossPoint WebSocket port, only used for the TCP fallback probe |
| `X3_HTTP_PORT` | `80` | CrossPoint HTTP port, used to list files, create folders and delete |
| `SETTLE_SECONDS` | `300` | Minimum age of the book's last modification before sending |
| `POLL_INTERVAL` | `30` | Seconds between checks |
| `SEED_EXISTING` | `true` | On first run, treat existing books as already sent |
| `OPTIMIZE` | `true` | Turn the X3 optimization on or off |
| `JPEG_QUALITY` | `85` | 1–100 |
| `GRAYSCALE` | `true` | |
| `AUTO_CROP` | `false` | Trim uniform image margins |
| `SPLIT_TEXT` | `true` | Split paragraphs and chapters, remove fonts |
| `UPLOAD_DIR` | `/` | Base folder on the X3; `PATH_TEMPLATE` folders go inside it |
| `PATH_TEMPLATE` | `{author}/{title}` | Folders and filename on the X3; each `/` is a folder level. Fields: `{author}` (first author), `{author_sort}` (e.g. `Herbert, Frank`), `{authors}` (all, joined with ` & `), `{title}`, `{id}`. `.epub` is added. Characters not valid on FAT, including `:` and a `/` inside a title, become `_` |
| `ASCII_NAMES` | `false` | Transliterate folder and file names to plain ASCII (`Søren` → `Soren`), in case the X3's file browser can't show some characters |
| `OVERWRITE_EXISTING` | `false` | If the file is already on the X3: `false` skips it, `true` replaces it |
| `UPLOAD_RETRIES` / `UPLOAD_RETRY_DELAY` / `SOCKET_TIMEOUT` | `3` / `2` / `30` | Upload retry and timeout settings |
| `CHUNK_SIZE` | `2048` | WebSocket frame size in bytes, capped at 2048 like the plugin. The X3 drops the connection on larger frames |
| `DEBUG` | `false` | Verbose log, including the WebSocket exchange |

## Housekeeping

- **Resend a book.** Delete its entry from `/config/state.json`. The file is
  re-read on every poll, so no restart is needed. If the old copy is still on
  the X3, also delete it there, or set `OVERWRITE_EXISTING=true`.
- **Changing `PATH_TEMPLATE`** only affects books sent from then on. Books
  already on the X3 stay where they are.
- **Run a single pass** for testing:
  `docker exec x3-sync python /app/x3sync.py --once`
- **"unable to open database file".** The library is probably in SQLite WAL
  mode, which a read-only mount can't open. Remove `:ro` from the library
  mapping. x3-sync still opens the database read-only.

## Credits

`crosspoint/optimizer.py`, `textsplit.py` and `ws_client.py` are unmodified
copies from [crosspoint-reader/calibre-plugins](https://github.com/crosspoint-reader/calibre-plugins).
They are MIT licensed (see `crosspoint/LICENSE`), and the upstream commit is
recorded in `crosspoint/UPSTREAM`.

x3sync.py uses its own upload loop. Upstream's `upload_file()` throws away
text frames it drains between chunks, and also loses a `DONE` that arrives
just before the device closes the connection, so a successful transfer can
hang until the socket times out or be reported as failed. Its default
16 KB frame size also fails on real hardware, so x3-sync sends 2 KB frames,
the same cap the plugin applies in `driver.py`. The folder,
existing-file and delete handling follows the plugin's `driver.py`, which
uses CrossPoint's HTTP API (`/api/files`, `/mkdir`, `/delete`).
