# Telegram Downloader

The workspace is now split into two implementations:

- [python/](/Users/pranavgajjewar/workspace/projects/telegram-downloader/python) contains the original Telethon-based downloader.
- [rust/](/Users/pranavgajjewar/workspace/projects/telegram-downloader/rust) contains the new Rust MTProto rewrite built with `grammers-client`.

## Python layout

The original files were moved under [python/](/Users/pranavgajjewar/workspace/projects/telegram-downloader/python):

- [python/telegram_downloader.py](/Users/pranavgajjewar/workspace/projects/telegram-downloader/python/telegram_downloader.py)
- [python/requirements-mtproto.txt](/Users/pranavgajjewar/workspace/projects/telegram-downloader/python/requirements-mtproto.txt)
- [python/.env.example](/Users/pranavgajjewar/workspace/projects/telegram-downloader/python/.env.example)

Run it from that directory:

```bash
cd python
python3 -m venv .venv-mtproto
source .venv-mtproto/bin/activate
pip install -r requirements-mtproto.txt
python telegram_downloader.py
```

## Rust layout

The Rust rewrite lives in [rust/](/Users/pranavgajjewar/workspace/projects/telegram-downloader/rust).

### Requirements

- Rust toolchain with `cargo`
- A Telegram `api_id` and `api_hash` from [my.telegram.org](https://my.telegram.org)
- A Telegram bot token from [@BotFather](https://t.me/BotFather)

### Configuration

The Rust binary loads `.env` from [rust/](/Users/pranavgajjewar/workspace/projects/telegram-downloader/rust). Start from [rust/.env.example](/Users/pranavgajjewar/workspace/projects/telegram-downloader/rust/.env.example).

- `TELEGRAM_API_ID` required Telegram API ID
- `TELEGRAM_API_HASH` required Telegram API hash
- `TELEGRAM_BOT_TOKEN` required bot token used for login
- `TELEGRAM_SESSION_NAME` session database path, default `./telegram_downloader.session`
- `DOWNLOAD_DIR` destination directory, default `./downloads-mtproto`
- `MAX_CONCURRENT_DOWNLOADS` parallel file downloads, default `2`
- `PARALLEL_CHUNK_DOWNLOADS` number of segment workers per file, default `4`
- `DOWNLOAD_CHUNK_SIZE_MB` per-request chunk size in MB, default `0.5`
- `LOG_LEVEL` logging level, default `INFO`
- `REPLY_ON_DUPLICATE` whether to reply for duplicates or in-progress items, default `true`

### Run

```bash
cd rust
cargo run
```

## Behavior

Both implementations target the same workflow:

- Watch incoming messages visible to the authenticated Telegram account
- Download photos and documents directly over MTProto
- Resume partial downloads under `DOWNLOAD_DIR/.partial`
- Refresh terminal progress on a single line while downloads are active
- Edit a Telegram progress reply periodically during downloads
- Reply on success, failure, or duplicate detection

The Rust implementation authenticates as a bot, so it only sees updates that Telegram delivers to bots.
