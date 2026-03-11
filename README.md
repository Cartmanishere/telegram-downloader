# Telegram Downloader

Rust Telegram media downloader built on `grammers-client`.

## What It Does

- Authenticates to Telegram as a bot
- Listens for incoming bot-visible messages with photos, documents, or supported links in message text/captions
- Downloads media directly over MTProto
- Delegates magnet links and direct `http` / `https` / `ftp` downloads to `aria2c`
- Resumes partial downloads from `DOWNLOAD_DIR/.partial`
- Sends Telegram replies for started, completed, failed, and duplicate downloads
- Prompts for `TV Show`, `Movie`, or `Anime` after each successful download and moves the payload into the configured library path

## Requirements

- Rust toolchain with `cargo`
- A Telegram API ID and API hash
- A Telegram bot token
- `aria2c` available on `PATH`, or configured via `ARIA2C_PATH`

## Configuration

The binary loads environment variables from `.env` in the current working directory.
If that file is absent, it falls back to `.env` next to the executable.

Required variables:

- `TELEGRAM_API_ID`: Telegram API ID
- `TELEGRAM_API_HASH`: Telegram API hash
- `TELEGRAM_BOT_TOKEN`: Bot token used to sign in
- `MOVIE_DIR`: Destination path for items classified as movies
- `TV_SHOW_DIR`: Destination path for items classified as TV shows
- `ANIME_DIR`: Destination path for items classified as anime

Optional variables:

- `TELEGRAM_SESSION_NAME`: Session database path. Relative paths are resolved from the loaded `.env` directory, or the current working directory if no `.env` file is loaded. Default: `telegram_downloader.session`
- `DOWNLOAD_DIR`: Download destination. Relative paths are resolved from the loaded `.env` directory, or the current working directory if no `.env` file is loaded. Default: `./downloads-mtproto`
- `MAX_CONCURRENT_DOWNLOADS`: Parallel file downloads. Default: `2`
- `PARALLEL_CHUNK_DOWNLOADS`: Segment workers per file. Default: `4`
- `DOWNLOAD_CHUNK_SIZE_MB`: Per-request chunk size in MB. Default: `0.5`
- `ARIA2C_PATH`: `aria2c` executable path. Default: `aria2c`
- `ARIA2C_POLL_INTERVAL_MS`: Poll interval for `aria2c` RPC progress updates. Default: `1000`
- `LOG_LEVEL`: Logging level. Default: `INFO`
- `REPLY_ON_DUPLICATE`: Reply when a file already exists or is already downloading. Default: `true`

Example `.env`:

```env
TELEGRAM_API_ID=12345678
TELEGRAM_API_HASH=0123456789abcdef0123456789abcdef
TELEGRAM_BOT_TOKEN=1234567890:AAExampleBotTokenValue
TELEGRAM_SESSION_NAME=telegram_downloader.session
DOWNLOAD_DIR=./downloads-mtproto
MOVIE_DIR=./library/movies
TV_SHOW_DIR=./library/tv
ANIME_DIR=./library/anime
MAX_CONCURRENT_DOWNLOADS=2
PARALLEL_CHUNK_DOWNLOADS=4
DOWNLOAD_CHUNK_SIZE_MB=0.5
ARIA2C_PATH=aria2c
ARIA2C_POLL_INTERVAL_MS=1000
LOG_LEVEL=INFO
REPLY_ON_DUPLICATE=true
```

## Getting Telegram Credentials

### `TELEGRAM_API_ID` and `TELEGRAM_API_HASH`

1. Go to [my.telegram.org](https://my.telegram.org).
2. Sign in with your Telegram account.
3. Open `API development tools`.
4. Create an application.
5. Copy the generated `api_id` and `api_hash` into `.env`.

These credentials belong to your Telegram account, not to BotFather.

### `TELEGRAM_BOT_TOKEN`

1. Open [@BotFather](https://t.me/BotFather) in Telegram.
2. Run `/newbot` and complete the bot setup.
3. Copy the bot token BotFather returns.
4. Put that value into `TELEGRAM_BOT_TOKEN` in `.env`.

If you already have a bot, you can use BotFather’s `/token` command to issue or view the token again.

## Run

```bash
cargo run
```

## Release Binaries

GitHub releases build and attach binaries for:

- Linux AMD64
- Linux ARM64
- macOS ARM64

## Notes

- This app signs in as a bot, so it only receives updates Telegram delivers to bots.
- Absolute paths in environment variables are used as-is.
- When a message has Telegram media, the app keeps using the built-in MTProto downloader.
- When a message has no downloadable Telegram media, the app scans raw text/caption content for:
  - the first `magnet:` URI, or
  - otherwise the first direct `http`, `https`, or `ftp` URL
- Direct-link downloads are written into `DOWNLOAD_DIR` using the URL filename when possible, or `download_<message_id>` as a fallback.
- Magnet downloads are stored under `DOWNLOAD_DIR/torrents/<info_hash>/`.
- After a successful download, the bot sends a Telegram prompt with `TV Show`, `Movie`, and `Anime` buttons. The original sender can also reply to that prompt with `TV Show`, `Movie`, or `Anime` as plain text.
- When a magnet finishes, the bot classifies and moves the single top-level payload inside `DOWNLOAD_DIR/torrents/<info_hash>/`. If that directory is empty or contains multiple top-level payload entries, the bot reports that classification cannot proceed automatically.
- The bot sends Telegram messages when a classified move starts and when it finishes or fails.
- Telegram `text_link` formatting entities are not parsed in this version; only raw pasted links in message text/captions are recognized.
