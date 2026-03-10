# Telegram Downloader

Rust Telegram media downloader built on `grammers-client`.

## What It Does

- Authenticates to Telegram as a bot
- Listens for incoming bot-visible messages with photos or documents
- Downloads media directly over MTProto
- Resumes partial downloads from `DOWNLOAD_DIR/.partial`
- Sends Telegram replies for started, completed, failed, and duplicate downloads

## Requirements

- Rust toolchain with `cargo`
- A Telegram API ID and API hash
- A Telegram bot token

## Configuration

The binary loads environment variables from `.env` in the project root.

Required variables:

- `TELEGRAM_API_ID`: Telegram API ID
- `TELEGRAM_API_HASH`: Telegram API hash
- `TELEGRAM_BOT_TOKEN`: Bot token used to sign in

Optional variables:

- `TELEGRAM_SESSION_NAME`: Session database path. Default: `telegram_downloader.session`
- `DOWNLOAD_DIR`: Download destination. Default: `./downloads-mtproto`
- `MAX_CONCURRENT_DOWNLOADS`: Parallel file downloads. Default: `2`
- `PARALLEL_CHUNK_DOWNLOADS`: Segment workers per file. Default: `4`
- `DOWNLOAD_CHUNK_SIZE_MB`: Per-request chunk size in MB. Default: `0.5`
- `LOG_LEVEL`: Logging level. Default: `INFO`
- `REPLY_ON_DUPLICATE`: Reply when a file already exists or is already downloading. Default: `true`

Example `.env`:

```env
TELEGRAM_API_ID=12345678
TELEGRAM_API_HASH=0123456789abcdef0123456789abcdef
TELEGRAM_BOT_TOKEN=1234567890:AAExampleBotTokenValue
TELEGRAM_SESSION_NAME=telegram_downloader.session
DOWNLOAD_DIR=./downloads-mtproto
MAX_CONCURRENT_DOWNLOADS=2
PARALLEL_CHUNK_DOWNLOADS=4
DOWNLOAD_CHUNK_SIZE_MB=0.5
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
- Relative paths in `.env` are resolved from the project root.
