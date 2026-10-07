#!/usr/bin/env python3
"""x3-sync: push new Calibre EPUBs to a CrossPoint-flashed Xteink X3.

Loop:
  1. Read the Calibre library's metadata.db (read-only) and queue every EPUB
     book whose uuid hasn't been sent yet and that hasn't been modified for
     SETTLE_SECONDS (so CWA has finished fetching metadata / converting).
  2. If the queue isn't empty, look for the X3 on the network. It only answers
     while it's on the File Transfer screen.
  3. When found: optimize each queued EPUB for the X3 (528x792, grayscale JPEG,
     paragraph/chapter splitting, font removal), upload it over CrossPoint's
     WebSocket protocol, and record it as sent.

All configuration is via environment variables (see README.md).
"""

import json
import logging
import os
import re
import select
import shutil
import signal
import socket
import sqlite3
import sys
import tempfile
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone

from crosspoint import ws_client
from crosspoint.optimizer import DEVICE_PROFILES, Options, optimize_epub


# --------------------------------------------------------------------------
# Configuration
# --------------------------------------------------------------------------

def _env_bool(name, default):
    val = os.environ.get(name)
    if val is None or val.strip() == '':
        return default
    return val.strip().lower() in ('1', 'true', 'yes', 'on')


def _env_int(name, default):
    val = os.environ.get(name)
    if val is None or val.strip() == '':
        return default
    return int(val)


class Config:
    def __init__(self):
        self.library_dir = os.environ.get('LIBRARY_DIR', '/calibre-library')
        self.state_dir = os.environ.get('STATE_DIR', '/config')
        # Fixed IP of the X3 (recommended: DHCP reservation). Empty = broadcast
        # discovery only, which needs host or br0 networking.
        self.x3_host = os.environ.get('X3_HOST', '').strip()
        self.x3_port = _env_int('X3_PORT', 81)          # WebSocket uploads
        self.x3_http_port = _env_int('X3_HTTP_PORT', 80)  # file list / mkdir / delete
        self.upload_dir = '/' + os.environ.get('UPLOAD_DIR', '/').strip().strip('/')
        self.poll_interval = _env_int('POLL_INTERVAL', 30)
        self.settle_seconds = _env_int('SETTLE_SECONDS', 300)
        self.seed_existing = _env_bool('SEED_EXISTING', True)
        # '/' in the template creates folders on the X3, one level at a time.
        self.path_template = os.environ.get('PATH_TEMPLATE', '{author}/{title}')
        self.ascii_names = _env_bool('ASCII_NAMES', False)
        # The firmware refuses to upload over an existing file: skip it (and
        # count the book as sent) or delete the old copy and upload again.
        self.overwrite_existing = _env_bool('OVERWRITE_EXISTING', False)
        self.optimize = _env_bool('OPTIMIZE', True)
        self.jpeg_quality = _env_int('JPEG_QUALITY', 85)
        self.grayscale = _env_bool('GRAYSCALE', True)
        self.auto_crop = _env_bool('AUTO_CROP', False)
        self.split_text = _env_bool('SPLIT_TEXT', True)
        self.retries = _env_int('UPLOAD_RETRIES', 3)
        self.retry_delay = _env_int('UPLOAD_RETRY_DELAY', 2)
        self.socket_timeout = _env_int('SOCKET_TIMEOUT', 30)
        # WebSocket frame size. The Calibre plugin hard-caps this at 2048: the
        # X3 drops the connection on bigger frames (16 KB, ws_client's default,
        # fails on real hardware), so the same cap applies here.
        self.chunk_size = max(256, min(_env_int('CHUNK_SIZE', 2048), 2048))
        self.debug = _env_bool('DEBUG', False)

    @property
    def db_path(self):
        return os.path.join(self.library_dir, 'metadata.db')

    @property
    def state_path(self):
        return os.path.join(self.state_dir, 'state.json')


VERSION = '0.4 (2 KB frames)'

log = logging.getLogger('x3-sync')


# --------------------------------------------------------------------------
# State: which books (by Calibre uuid) are already on the X3
# --------------------------------------------------------------------------

class State:
    def __init__(self, path):
        self.path = path
        self.sent = {}       # uuid -> {"title", "file", "at"}
        self.failed = {}     # uuid -> consecutive failure count
        self.exists = os.path.exists(path)
        if self.exists:
            with open(path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            self.sent = data.get('sent', {})
            self.failed = data.get('failed', {})

    def save(self):
        os.makedirs(os.path.dirname(self.path), exist_ok=True)
        tmp = self.path + '.tmp'
        with open(tmp, 'w', encoding='utf-8') as f:
            json.dump({'sent': self.sent, 'failed': self.failed}, f,
                      indent=2, ensure_ascii=False)
        os.replace(tmp, self.path)
        self.exists = True

    def mark_sent(self, book, filename):
        self.sent[book['uuid']] = {
            'title': book['title'],
            'file': filename,
            'at': datetime.now(timezone.utc).isoformat(timespec='seconds'),
        }
        self.failed.pop(book['uuid'], None)


# --------------------------------------------------------------------------
# Calibre library
# --------------------------------------------------------------------------

BOOKS_SQL = """
SELECT b.id, b.uuid, b.title, b.author_sort, b.path, b.last_modified, d.name,
       (SELECT group_concat(a.name, ' & ')
          FROM books_authors_link bal JOIN authors a ON a.id = bal.author
         WHERE bal.book = b.id) AS authors
  FROM books b
  JOIN data d ON d.book = b.id AND upper(d.format) = 'EPUB'
 ORDER BY b.id
"""


def _parse_ts(value):
    if not value:
        return None
    try:
        ts = datetime.fromisoformat(str(value))
    except ValueError:
        return None
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return ts


def read_library(cfg):
    """Return every EPUB book in the library as a list of dicts."""
    uri = 'file:%s?mode=ro' % cfg.db_path
    conn = sqlite3.connect(uri, uri=True, timeout=10)
    try:
        rows = conn.execute(BOOKS_SQL).fetchall()
    finally:
        conn.close()
    books = []
    for book_id, uuid, title, author_sort, path, last_modified, name, authors in rows:
        books.append({
            'id': book_id,
            'uuid': uuid,
            'title': title or 'Unknown',
            'authors': authors or 'Unknown',
            'author_sort': author_sort or authors or 'Unknown',
            'last_modified': _parse_ts(last_modified),
            'file': os.path.join(cfg.library_dir, path, name + '.epub'),
        })
    return books


_BAD_CHARS = re.compile(r'[\\/:*?"<>|\x00-\x1f]')
# Letters that NFKD doesn't split into base letter + accent.
_TRANSLIT = str.maketrans({
    'ø': 'o', 'Ø': 'O', 'æ': 'ae', 'Æ': 'AE', 'œ': 'oe', 'Œ': 'OE', 'ß': 'ss',
    'đ': 'd', 'Đ': 'D', 'ł': 'l', 'Ł': 'L', 'þ': 'th', 'Þ': 'Th', 'ð': 'd', 'Ð': 'D',
    '’': "'", '‘': "'", '“': '"', '”': '"', '–': '-', '—': '-', '…': '...',
})


def _clean_component(cfg, text):
    # ':' is also the field separator of the START: message, so it must go.
    if cfg.ascii_names:
        import unicodedata
        text = unicodedata.normalize('NFKD', text.translate(_TRANSLIT))
        text = text.encode('ascii', 'ignore').decode('ascii')
    text = _BAD_CHARS.sub('_', text)
    return re.sub(r'\s+', ' ', text).strip(' .')[:100]


def device_path(cfg, book):
    """Return (folders, filename) on the X3 for this book."""
    fields = {
        'author': book['authors'].split(' & ')[0],
        'authors': book['authors'],
        'author_sort': book['author_sort'].split(' & ')[0],
        'title': book['title'],
        'id': book['id'],
    }
    # Format each '/'-separated part separately, so a '/' inside a title
    # (e.g. "Either/Or") can't create an extra folder.
    parts = [_clean_component(cfg, p.format(**fields))
             for p in cfg.path_template.split('/')]
    parts = [p for p in parts if p]
    if not parts:
        parts = ['book-%d' % book['id']]
    return parts[:-1], parts[-1] + '.epub'


# --------------------------------------------------------------------------
# Device
# --------------------------------------------------------------------------

def find_device(cfg):
    """Return (host, port) if the X3 is reachable right now, else (None, None)."""
    # 1. CrossPoint's own UDP discovery. Unicast to X3_HOST works on a plain
    #    Docker bridge; broadcast needs host/br0 networking.
    extra = [cfg.x3_host] if cfg.x3_host else []
    try:
        host, port = ws_client.discover_device(
            timeout=1.0, debug=cfg.debug, logger=log.debug, extra_hosts=extra)
        if host:
            return host, port
    except Exception as exc:  # never let discovery kill the loop
        log.debug('UDP discovery error: %s', exc)

    # 2. Fallback: is the WebSocket port open on the known address?
    if cfg.x3_host:
        try:
            with socket.create_connection((cfg.x3_host, cfg.x3_port), timeout=2):
                return cfg.x3_host, cfg.x3_port
        except OSError:
            pass
    return None, None


def optimize_for_x3(cfg, src, workdir):
    """Return the path to send: an optimized copy, or src if disabled/failed."""
    if not cfg.optimize:
        return src
    out = os.path.join(workdir, 'optimized.epub')
    opts = Options(quality=cfg.jpeg_quality, grayscale=cfg.grayscale,
                   auto_crop=cfg.auto_crop, split_text=cfg.split_text)

    def step(tag, message):
        if tag in ('DONE', 'IMG-ERR', 'SPLIT-ERR'):
            log.info('    %s', message)
        else:
            log.debug('    [%s] %s', tag, message)

    try:
        optimize_epub(src, out, DEVICE_PROFILES['X3'], opts, log_fn=step)
        return out
    except Exception as exc:
        # Same policy as the plugin: never block a transfer on optimization.
        log.warning('    optimization failed (%s), sending original', exc)
        return src


def _join(folder, name):
    return '/' + name if folder == '/' else folder + '/' + name


class Device:
    """CrossPoint's HTTP file API (port 80), as used by the Calibre plugin."""

    def __init__(self, cfg, host):
        self.cfg = cfg
        self.base = 'http://%s:%d' % (host, cfg.x3_http_port)

    def list_dir(self, folder):
        url = self.base + '/api/files?' + urllib.parse.urlencode({'path': folder})
        with urllib.request.urlopen(url, timeout=10) as resp:
            return json.loads(resp.read().decode('utf-8', 'ignore'))

    def _post(self, endpoint, data):
        body = urllib.parse.urlencode(data).encode('utf-8')
        req = urllib.request.Request(
            self.base + endpoint, data=body, method='POST',
            headers={'Content-Type': 'application/x-www-form-urlencoded'})
        with urllib.request.urlopen(req, timeout=10) as resp:
            resp.read()

    def _entry(self, folder, name, is_dir):
        try:
            entries = self.list_dir(folder)
        except Exception:
            return None
        for e in entries:
            if e.get('name') == name and bool(e.get('isDirectory')) == is_dir:
                return e
        return None

    def ensure_folders(self, folders):
        """Create each level that's missing, one mkdir per level (like the
        plugin: deep paths in one call and mkdir on an existing folder are
        unreliable on some firmware). Returns the full folder path."""
        current = self.cfg.upload_dir
        for name in folders:
            if self._entry(current, name, True) is None:
                try:
                    self._post('/mkdir', {'name': name, 'path': current})
                    log.debug('    mkdir %s', _join(current, name))
                except Exception as exc:
                    if self._entry(current, name, True) is None:
                        raise RuntimeError('mkdir %s failed: %s' % (_join(current, name), exc))
            current = _join(current, name)
        return current

    def file_exists(self, folder, filename):
        return self._entry(folder, filename, False) is not None

    def delete(self, path):
        self._post('/delete', {'paths': json.dumps([path])})


def _close_reason(payload):
    if len(payload) >= 2:
        code = int.from_bytes(payload[:2], 'big')
        reason = payload[2:].decode('utf-8', 'ignore').strip()
        return 'close frame %d%s' % (code, (' "%s"' % reason) if reason else '')
    return 'close frame without code'


def _drain(client):
    """Read pending text frames without blocking.

    Returns (messages, closed), where closed is False or a description of
    how the connection ended. Unlike ws_client.drain_messages, messages
    read before the connection closes are kept, so a DONE followed by an
    immediate close isn't lost.
    """
    msgs = []
    while True:
        try:
            ready, _, _ = select.select([client.sock], [], [], 0)
            if not ready:
                return msgs, False
            opcode, payload = client._read_frame()
        except (ws_client.WebSocketError, OSError, ValueError) as exc:
            return msgs, 'connection lost (%s)' % exc
        if opcode == 0x8:
            return msgs, _close_reason(payload)
        if opcode == 0x9:
            client._send_frame(0xA, payload)
        elif opcode == 0x1:
            msgs.append(payload.decode('utf-8', 'ignore'))


def _upload_once(cfg, host, port, folder, filename, path):
    """CrossPoint upload: START -> READY -> binary frames -> DONE.

    The firmware also sends PROGRESS:<received>:<total> every 64 KB and on the
    last chunk, and ERROR:<message> on failure. Differences from
    ws_client.upload_file: frames are at most cfg.chunk_size (2 KB) like the
    plugin, text frames drained between chunks are checked instead of thrown
    away, and the close code/reason is reported when the X3 hangs up.
    """
    client = ws_client.WebSocketClient(host, port, timeout=cfg.socket_timeout,
                                       debug=cfg.debug, logger=log.debug)
    done = closed = False

    def handle(msgs):
        nonlocal done
        for m in msgs:
            if m.startswith('ERROR'):
                raise ws_client.WebSocketError('X3 says ' + m)
            if m == 'DONE':
                done = True
            elif m.startswith('PROGRESS:'):
                log.debug('    %s', m)

    try:
        client.connect()
        size = os.path.getsize(path)
        client.send_text('START:%s:%d:%s' % (filename, size, folder))
        msg = client.read_text()
        if msg != 'READY':
            raise ws_client.WebSocketError('X3 says %s' % (msg or '<empty>'))

        sent = 0
        with open(path, 'rb') as f:
            while not closed:
                chunk = f.read(cfg.chunk_size)
                if not chunk:
                    break
                try:
                    client.send_binary(chunk)
                except ws_client.WebSocketError as exc:
                    # Pick up an ERROR or close frame the X3 sent first.
                    msgs, closed = _drain(client)
                    handle(msgs)
                    raise ws_client.WebSocketError('%s after %d of %d bytes%s' % (
                        exc, sent, size, ' (%s)' % closed if closed else ''))
                sent += len(chunk)
                msgs, closed = _drain(client)
                handle(msgs)
        if done:
            return
        if closed:
            raise ws_client.WebSocketError('X3 closed the connection after %d of %d bytes: %s'
                                           % (sent, size, closed))
        while not done:
            handle([client.read_text()])
    except OSError as exc:
        raise ws_client.WebSocketError(str(exc)) from exc
    finally:
        client.close()


def upload(cfg, device, host, port, folder, filename, path):
    target = _join(folder, filename)
    last = None
    for attempt in range(1, cfg.retries + 1):
        try:
            _upload_once(cfg, host, port, folder, filename, path)
            return
        except ws_client.WebSocketError as exc:
            last = exc
            log.warning('    upload attempt %d/%d failed: %s', attempt, cfg.retries, exc)
            # A partial file would make every later attempt fail (the firmware
            # won't upload over an existing file), so remove it.
            try:
                if device.file_exists(folder, filename):
                    device.delete(target)
                    log.info('    removed partial upload %s', target)
            except Exception as cleanup_exc:
                log.debug('    partial-upload cleanup failed: %s', cleanup_exc)
            if attempt < cfg.retries:
                time.sleep(cfg.retry_delay)
    raise last


def send_book(cfg, host, port, book):
    """Send one book. Returns its path on the X3."""
    device = Device(cfg, host)
    folders, filename = device_path(cfg, book)
    folder = device.ensure_folders(folders)
    target = _join(folder, filename)
    log.info('  %s', target)

    if device.file_exists(folder, filename):
        if not cfg.overwrite_existing:
            log.info('    already on the X3, skipping (OVERWRITE_EXISTING=false)')
            return target
        device.delete(target)
        log.info('    already on the X3, replacing it')

    workdir = tempfile.mkdtemp(prefix='x3sync-')
    try:
        path = optimize_for_x3(cfg, book['file'], workdir)
        upload(cfg, device, host, port, folder, filename, path)
    finally:
        shutil.rmtree(workdir, ignore_errors=True)
    return target


# --------------------------------------------------------------------------
# Main loop
# --------------------------------------------------------------------------

def pending_books(cfg, state, books):
    now = datetime.now(timezone.utc)
    ready, settling = [], 0
    for b in books:
        if b['uuid'] in state.sent:
            continue
        if b['last_modified'] and (now - b['last_modified']).total_seconds() < cfg.settle_seconds:
            settling += 1
            continue
        ready.append(b)
    return ready, settling


def sync_once(cfg, state):
    books = read_library(cfg)

    if not state.exists:
        if cfg.seed_existing:
            for b in books:
                state.mark_sent(b, '(already in library at first run)')
            log.info('First run: marked %d existing book(s) as already on the X3. '
                     'Only books added from now on will be sent.', len(books))
        else:
            log.info('First run: SEED_EXISTING=false, the whole library (%d) is queued.',
                     len(books))
        state.save()
        if cfg.seed_existing:
            return

    queue, settling = pending_books(cfg, state, books)
    if not queue:
        if settling:
            log.debug('%d book(s) still settling', settling)
        return

    host, port = find_device(cfg)
    if not host:
        log.debug('%d book(s) queued, X3 not in File Transfer mode', len(queue))
        return

    log.info('X3 found at %s:%d, sending %d book(s)', host, port, len(queue))
    for book in queue:
        if not os.path.isfile(book['file']):
            log.warning('Missing file for "%s", skipping: %s', book['title'], book['file'])
            continue
        try:
            target = send_book(cfg, host, port, book)
        except Exception as exc:
            state.failed[book['uuid']] = state.failed.get(book['uuid'], 0) + 1
            state.save()
            log.error('  failed: %s (will retry next time the X3 is seen)', exc)
            # The device most likely left transfer mode; stop this round.
            return
        state.mark_sent(book, target)
        state.save()
        log.info('  sent')
        time.sleep(1)  # same pacing between books as the plugin
    log.info('Done, queue empty')


def main():
    cfg = Config()
    logging.basicConfig(
        level=logging.DEBUG if cfg.debug else logging.INFO,
        format='%(asctime)s %(levelname)-7s %(message)s',
        stream=sys.stdout)

    if not os.path.isfile(cfg.db_path):
        log.error('No metadata.db in %s - is LIBRARY_DIR mounted correctly?', cfg.library_dir)
        sys.exit(1)

    log.info('x3-sync %s starting: library=%s host=%s optimize=%s (X3 528x792, q=%d, '
             'grayscale=%s, crop=%s, split=%s)',
             VERSION, cfg.library_dir, cfg.x3_host or '(broadcast)', cfg.optimize,
             cfg.jpeg_quality, cfg.grayscale, cfg.auto_crop, cfg.split_text)

    stop = {'flag': False}
    signal.signal(signal.SIGTERM, lambda *_: stop.update(flag=True))
    signal.signal(signal.SIGINT, lambda *_: stop.update(flag=True))

    once = '--once' in sys.argv
    while not stop['flag']:
        state = State(cfg.state_path)  # re-read so hand edits take effect
        try:
            sync_once(cfg, state)
        except sqlite3.Error as exc:
            log.warning('Could not read metadata.db (%s), retrying', exc)
        except Exception:
            log.exception('Unexpected error, continuing')
        if once:
            break
        for _ in range(cfg.poll_interval):
            if stop['flag']:
                break
            time.sleep(1)
    log.info('x3-sync stopped')


if __name__ == '__main__':
    main()
