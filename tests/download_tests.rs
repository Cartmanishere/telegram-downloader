mod common;

use anyhow::bail;
use common::{copy_dir, remove_source_files, test_config, unique_test_dir};
use std::ffi::OsStr;
use std::fs;
use std::path::PathBuf;
use telegram_downloader_rust::download::{
    build_external_target, choose_available_path, contains_payload_file, extract_external_link,
    finalize_mega_target, infer_common_payload_root, infer_mega_wrapper_name,
    move_classified_target, move_classified_target_with_operations, parse_human_size,
    parse_megadl_progress_line, resolve_magnet_payload, resolve_mega_payload, ActiveDownloadKey,
    ClassificationTarget, ExternalSource, LibraryKind,
};

#[test]
fn extracts_direct_http_link() {
    let source = extract_external_link("download https://example.com/files/archive.zip")
        .expect("http link should be found");
    assert_eq!(
        source,
        ExternalSource::DirectUrl {
            url: "https://example.com/files/archive.zip".to_string()
        }
    );
}

#[test]
fn extracts_https_link_with_query() {
    let source = extract_external_link("mirror: https://example.com/file.iso?token=abc123")
        .expect("https link should be found");
    assert_eq!(
        source,
        ExternalSource::DirectUrl {
            url: "https://example.com/file.iso?token=abc123".to_string()
        }
    );
}

#[test]
fn extracts_mega_link_before_direct_url() {
    let source = extract_external_link("download https://mega.nz/folder/abc#def")
        .expect("mega link should be found");
    assert_eq!(
        source,
        ExternalSource::Mega {
            url: "https://mega.nz/folder/abc#def".to_string(),
        }
    );
}

#[test]
fn prefers_magnet_over_direct_link() {
    let source = extract_external_link(
        "http://example.com/file.iso magnet:?xt=urn:btih:ABCDEF1234567890&dn=test",
    )
    .expect("magnet should be found");
    assert_eq!(
        source,
        ExternalSource::Magnet {
            uri: "magnet:?xt=urn:btih:ABCDEF1234567890&dn=test".to_string(),
            info_hash: "abcdef1234567890".to_string(),
        }
    );
}

#[test]
fn returns_none_when_no_supported_link_exists() {
    assert!(extract_external_link("nothing to download here").is_none());
}

#[test]
fn builds_direct_target_from_url_filename() {
    let base = unique_test_dir("direct-target");
    fs::create_dir_all(&base).expect("test dir should be created");
    let (label, target, expected_size) = build_external_target(
        &base,
        42,
        &ExternalSource::DirectUrl {
            url: "https://example.com/release.tar.gz".to_string(),
        },
    )
    .expect("target should build");
    assert_eq!(label, "release.tar.gz");
    assert_eq!(target, base.join("release.tar.gz"));
    assert_eq!(expected_size, None);
    let _ = fs::remove_dir_all(&base);
}

#[test]
fn falls_back_to_message_id_when_url_has_no_filename() {
    let base = unique_test_dir("direct-fallback");
    fs::create_dir_all(&base).expect("test dir should be created");
    let (label, target, _) = build_external_target(
        &base,
        99,
        &ExternalSource::DirectUrl {
            url: "https://example.com/downloads/".to_string(),
        },
    )
    .expect("fallback target should build");
    assert_eq!(label, "download_99");
    assert_eq!(target, base.join("download_99"));
    let _ = fs::remove_dir_all(&base);
}

#[test]
fn builds_magnet_directory_from_info_hash() {
    let base = PathBuf::from("/tmp/test-downloads");
    let (label, target, expected_size) = build_external_target(
        &base,
        1,
        &ExternalSource::Magnet {
            uri: "magnet:?xt=urn:btih:001122".to_string(),
            info_hash: "001122".to_string(),
        },
    )
    .expect("magnet target should build");
    assert_eq!(label, "torrent_001122");
    assert_eq!(target, base.join("torrents").join("001122"));
    assert_eq!(expected_size, None);
}

#[test]
fn builds_mega_directory_from_message_id() {
    let base = PathBuf::from("/tmp/test-downloads");
    let (label, target, expected_size) = build_external_target(
        &base,
        77,
        &ExternalSource::Mega {
            url: "https://mega.nz/file/test".to_string(),
        },
    )
    .expect("mega target should build");
    assert_eq!(label, "mega_77");
    assert_eq!(target, base.join("mega").join("mega_77"));
    assert_eq!(expected_size, None);
}

#[test]
fn active_keys_cover_telegram_and_external_downloads() {
    let telegram = ActiveDownloadKey::Telegram {
        path: PathBuf::from("/tmp/photo.jpg"),
        size: Some(10),
    };
    let direct = ActiveDownloadKey::External {
        identity: "url:https://example.com/file.bin".to_string(),
    };
    let magnet = ActiveDownloadKey::External {
        identity: "magnet:001122".to_string(),
    };
    let mega = ActiveDownloadKey::External {
        identity: "mega:https://mega.nz/file/test".to_string(),
    };

    assert_eq!(
        telegram,
        ActiveDownloadKey::Telegram {
            path: PathBuf::from("/tmp/photo.jpg"),
            size: Some(10),
        }
    );
    assert_eq!(
        direct,
        ActiveDownloadKey::External {
            identity: "url:https://example.com/file.bin".to_string(),
        }
    );
    assert_eq!(
        magnet,
        ActiveDownloadKey::External {
            identity: "magnet:001122".to_string(),
        }
    );
    assert_eq!(
        mega,
        ActiveDownloadKey::External {
            identity: "mega:https://mega.nz/file/test".to_string(),
        }
    );
}

#[test]
fn payload_detection_ignores_aria2_artifacts() {
    let base = unique_test_dir("payload-check");
    fs::create_dir_all(&base).expect("test dir should be created");
    fs::write(base.join("partial.iso.aria2"), b"state").expect("artifact should be written");
    assert!(!contains_payload_file(&base).expect("artifact-only dir should be empty"));

    fs::write(base.join("real.iso"), b"payload").expect("payload should be written");
    assert!(contains_payload_file(&base).expect("payload should be detected"));
    let _ = fs::remove_dir_all(&base);
}

#[test]
fn resolve_magnet_payload_returns_single_file() {
    let base = unique_test_dir("magnet-payload-file");
    let container = base.join("hash");
    fs::create_dir_all(&container).expect("container should exist");
    let payload = container.join("episode.mkv");
    fs::write(&payload, b"payload").expect("payload should be written");

    let target = resolve_magnet_payload(&container).expect("payload should resolve");
    assert_eq!(
        target,
        ClassificationTarget {
            source_path: payload,
            cleanup_dir: Some(container),
        }
    );
    let _ = fs::remove_dir_all(&base);
}

#[test]
fn resolve_magnet_payload_returns_single_directory() {
    let base = unique_test_dir("magnet-payload-dir");
    let container = base.join("hash");
    let payload = container.join("Season 1");
    fs::create_dir_all(&payload).expect("payload dir should exist");

    let target = resolve_magnet_payload(&container).expect("payload should resolve");
    assert_eq!(target.source_path, payload);
    assert_eq!(target.cleanup_dir, Some(container));
    let _ = fs::remove_dir_all(&base);
}

#[test]
fn resolve_magnet_payload_rejects_empty_directory() {
    let base = unique_test_dir("magnet-empty");
    let container = base.join("hash");
    fs::create_dir_all(&container).expect("container should exist");

    let error = resolve_magnet_payload(&container).expect_err("empty container should fail");
    assert!(error.to_string().contains("empty"));
    let _ = fs::remove_dir_all(&base);
}

#[test]
fn resolve_magnet_payload_rejects_multiple_top_level_entries() {
    let base = unique_test_dir("magnet-many");
    let container = base.join("hash");
    fs::create_dir_all(&container).expect("container should exist");
    fs::write(container.join("a.mkv"), b"a").expect("first payload should be written");
    fs::write(container.join("b.mkv"), b"b").expect("second payload should be written");

    let error = resolve_magnet_payload(&container).expect_err("multi-entry container should fail");
    assert!(error.to_string().contains("ambiguous"));
    let _ = fs::remove_dir_all(&base);
}

#[test]
fn resolve_mega_payload_returns_single_file() {
    let base = unique_test_dir("mega-payload-file");
    fs::create_dir_all(&base).expect("wrapper should exist");
    let payload = base.join("movie.mkv");
    fs::write(&payload, b"payload").expect("payload should exist");

    let target = resolve_mega_payload(&base).expect("payload should resolve");
    assert_eq!(
        target,
        ClassificationTarget {
            source_path: payload,
            cleanup_dir: Some(base),
        }
    );
}

#[test]
fn resolve_mega_payload_returns_single_directory() {
    let base = unique_test_dir("mega-payload-dir");
    let payload = base.join("Season 1");
    fs::create_dir_all(&payload).expect("payload dir should exist");

    let target = resolve_mega_payload(&base).expect("payload should resolve");
    assert_eq!(
        target,
        ClassificationTarget {
            source_path: payload,
            cleanup_dir: Some(base),
        }
    );
}

#[test]
fn resolve_mega_payload_rejects_multiple_top_level_entries() {
    let base = unique_test_dir("mega-many");
    fs::create_dir_all(&base).expect("wrapper should exist");
    fs::write(base.join("a.mkv"), b"a").expect("first payload should exist");
    fs::write(base.join("b.mkv"), b"b").expect("second payload should exist");

    let target =
        resolve_mega_payload(&base).expect("multi-entry wrapper should fall back to wrapper");
    assert_eq!(
        target,
        ClassificationTarget {
            source_path: base.clone(),
            cleanup_dir: None,
        }
    );
    let _ = fs::remove_dir_all(&base);
}

#[test]
fn mega_wrapper_name_uses_single_file_stem() {
    let base = unique_test_dir("mega-name-file");
    fs::create_dir_all(&base).expect("wrapper dir should exist");
    fs::write(base.join("Movie.Name.2021.mkv"), b"payload").expect("payload should exist");

    let name = infer_mega_wrapper_name(&base).expect("name should be inferred");
    assert_eq!(name, "Movie.Name.2021");
    let _ = fs::remove_dir_all(&base);
}

#[test]
fn mega_wrapper_name_uses_single_directory_name() {
    let base = unique_test_dir("mega-name-dir");
    let payload = base.join("Season 1");
    fs::create_dir_all(&payload).expect("payload dir should exist");

    let name = infer_mega_wrapper_name(&base).expect("name should be inferred");
    assert_eq!(name, "Season_1");
    let _ = fs::remove_dir_all(&base);
}

#[test]
fn mega_wrapper_name_uses_common_payload_root() {
    let base = unique_test_dir("mega-common-root");
    let payload = base.join("Show Name");
    fs::create_dir_all(&payload).expect("payload dir should exist");
    fs::write(payload.join("Episode 1.mkv"), b"one").expect("first payload");
    fs::write(payload.join("Episode 2.mkv"), b"two").expect("second payload");

    let common_root = infer_common_payload_root(&base).expect("common root should exist");
    assert_eq!(common_root, "Show_Name");
    let _ = fs::remove_dir_all(&base);
}

#[test]
fn mega_wrapper_name_falls_back_when_no_stable_name_exists() {
    let base = unique_test_dir("mega-fallback");
    fs::create_dir_all(&base).expect("wrapper dir should exist");
    fs::write(base.join("a.mkv"), b"a").expect("first payload should exist");
    fs::write(base.join("b.mkv"), b"b").expect("second payload should exist");

    assert!(infer_mega_wrapper_name(&base).is_none());
    let _ = fs::remove_dir_all(&base);
}

#[tokio::test]
async fn finalizing_mega_target_collapses_single_nested_directory() {
    let base = unique_test_dir("mega-collapse");
    let wrapper = base.join("mega_128_1");
    let nested = wrapper.join("mega_128_1");
    fs::create_dir_all(&nested).expect("nested wrapper should exist");
    fs::write(nested.join("episode.mkv"), b"payload").expect("payload should exist");

    let final_path = finalize_mega_target(&wrapper)
        .await
        .expect("mega finalization should succeed");

    assert_eq!(final_path, wrapper);
    assert!(wrapper.join("episode.mkv").exists());
    assert!(!wrapper.join("mega_128_1").exists());
    let _ = fs::remove_dir_all(&base);
}

#[test]
fn parses_megadl_progress_line_with_explicit_bytes() {
    let snapshot = parse_megadl_progress_line(
        "Cuella (2021).mkv: 0.02% - 267.8 KiB (274,176 bytes) of 1.7 GiB (242.2 KiB/s)",
    )
    .expect("progress line should parse");

    assert_eq!(snapshot.current_item_label, "Cuella (2021).mkv");
    assert_eq!(snapshot.downloaded_bytes, 274_176);
    assert_eq!(snapshot.total_bytes, Some(1_825_361_101));
    let speed = snapshot.speed_mb_s.expect("speed should be parsed");
    let expected = 248_013_f64 / (1024.0 * 1024.0);
    assert!((speed - expected).abs() < 1e-9);
}

#[test]
fn parses_human_sizes_with_iec_units() {
    assert_eq!(parse_human_size("267.8 KiB"), Some(274_227));
    assert_eq!(parse_human_size("1.7 GiB"), Some(1_825_361_101));
    assert_eq!(parse_human_size("242.2 KiB"), Some(248_013));
}

#[test]
fn megadl_progress_parser_tracks_file_switches() {
    let first = parse_megadl_progress_line(
        "one.mkv: 100.00% - 1.0 MiB (1,048,576 bytes) of 1.0 MiB (1.0 MiB/s)",
    )
    .expect("first line should parse");
    let second = parse_megadl_progress_line(
        "two.mkv: 5.00% - 50.0 MiB (52,428,800 bytes) of 100.0 MiB (10.0 MiB/s)",
    )
    .expect("second line should parse");

    assert_eq!(first.current_item_label, "one.mkv");
    assert_eq!(second.current_item_label, "two.mkv");
    assert_eq!(second.downloaded_bytes, 52_428_800);
}

#[test]
fn choose_available_path_adds_numeric_suffix() {
    let base = unique_test_dir("available-path");
    fs::create_dir_all(&base).expect("base should exist");
    fs::write(base.join("movie.mkv"), b"payload").expect("existing file should be written");

    let chosen = choose_available_path(&base, OsStr::new("movie.mkv"));
    assert_eq!(chosen, base.join("movie_1.mkv"));
    let _ = fs::remove_dir_all(&base);
}

#[test]
fn move_classified_target_moves_file_to_movie_dir() {
    let base = unique_test_dir("move-file");
    let config = test_config(&base);
    fs::create_dir_all(&config.movie_dir).expect("movie dir should exist");
    fs::create_dir_all(&config.tv_show_dir).expect("tv dir should exist");
    fs::create_dir_all(&config.anime_dir).expect("anime dir should exist");
    let source = base.join("downloads").join("movie.mkv");
    fs::create_dir_all(source.parent().unwrap()).expect("downloads dir should exist");
    fs::write(&source, b"payload").expect("source should exist");

    let destination = move_classified_target(
        &config,
        &ClassificationTarget {
            source_path: source.clone(),
            cleanup_dir: None,
        },
        LibraryKind::Movie,
    )
    .expect("move should succeed");

    assert_eq!(destination, config.movie_dir.join("movie.mkv"));
    assert!(destination.exists());
    assert!(!source.exists());
    let _ = fs::remove_dir_all(&base);
}

#[test]
fn move_classified_target_removes_empty_magnet_container() {
    let base = unique_test_dir("move-magnet");
    let config = test_config(&base);
    fs::create_dir_all(&config.movie_dir).expect("movie dir should exist");
    fs::create_dir_all(&config.tv_show_dir).expect("tv dir should exist");
    fs::create_dir_all(&config.anime_dir).expect("anime dir should exist");
    let container = base.join("downloads").join("torrents").join("abc");
    fs::create_dir_all(&container).expect("container should exist");
    let source = container.join("Show");
    fs::create_dir_all(&source).expect("payload dir should exist");

    let destination = move_classified_target(
        &config,
        &ClassificationTarget {
            source_path: source.clone(),
            cleanup_dir: Some(container.clone()),
        },
        LibraryKind::TvShow,
    )
    .expect("move should succeed");

    assert_eq!(destination, config.tv_show_dir.join("Show"));
    assert!(destination.exists());
    assert!(!container.exists());
    let _ = fs::remove_dir_all(&base);
}

#[test]
fn move_classified_target_avoids_overwrite() {
    let base = unique_test_dir("move-collision");
    let config = test_config(&base);
    fs::create_dir_all(&config.movie_dir).expect("movie dir should exist");
    fs::create_dir_all(&config.tv_show_dir).expect("tv dir should exist");
    fs::create_dir_all(&config.anime_dir).expect("anime dir should exist");
    fs::write(config.movie_dir.join("movie.mkv"), b"existing").expect("existing file");
    let source = base.join("downloads").join("movie.mkv");
    fs::create_dir_all(source.parent().unwrap()).expect("downloads dir should exist");
    fs::write(&source, b"payload").expect("source should exist");

    let destination = move_classified_target(
        &config,
        &ClassificationTarget {
            source_path: source,
            cleanup_dir: None,
        },
        LibraryKind::Movie,
    )
    .expect("move should succeed");

    assert_eq!(destination, config.movie_dir.join("movie_1.mkv"));
    let _ = fs::remove_dir_all(&base);
}

#[test]
fn move_classified_target_moves_directory_to_anime_dir() {
    let base = unique_test_dir("move-anime");
    let config = test_config(&base);
    fs::create_dir_all(&config.movie_dir).expect("movie dir should exist");
    fs::create_dir_all(&config.tv_show_dir).expect("tv dir should exist");
    fs::create_dir_all(&config.anime_dir).expect("anime dir should exist");
    let source = base.join("downloads").join("Anime Series");
    fs::create_dir_all(&source).expect("anime source should exist");

    let destination = move_classified_target(
        &config,
        &ClassificationTarget {
            source_path: source.clone(),
            cleanup_dir: None,
        },
        LibraryKind::Anime,
    )
    .expect("move should succeed");

    assert_eq!(destination, config.anime_dir.join("Anime Series"));
    assert!(destination.exists());
    assert!(!source.exists());
    let _ = fs::remove_dir_all(&base);
}

#[test]
fn move_classified_target_uses_external_mover_for_successful_transfer() {
    let base = unique_test_dir("move-rsync-success");
    let config = test_config(&base);
    fs::create_dir_all(&config.movie_dir).expect("movie dir should exist");
    fs::create_dir_all(&config.tv_show_dir).expect("tv dir should exist");
    fs::create_dir_all(&config.anime_dir).expect("anime dir should exist");
    let container = base.join("downloads").join("torrents").join("abc");
    fs::create_dir_all(&container).expect("container should exist");
    let source = container.join("Show");
    fs::create_dir_all(&source).expect("payload dir should exist");
    fs::write(source.join("episode.mkv"), b"payload").expect("payload file should exist");

    let destination = move_classified_target_with_operations(
        &config,
        &ClassificationTarget {
            source_path: source.clone(),
            cleanup_dir: Some(container.clone()),
        },
        LibraryKind::Movie,
        &|source, destination| {
            copy_dir(source, destination)?;
            remove_source_files(source)?;
            Ok(())
        },
    )
    .expect("rsync-style move should succeed");

    assert_eq!(destination, config.movie_dir.join("Show"));
    assert!(destination.join("episode.mkv").exists());
    assert!(!source.exists());
    assert!(!container.exists());
    let _ = fs::remove_dir_all(&base);
}

#[test]
fn move_classified_target_keeps_cleanup_dir_when_external_mover_fails() {
    let base = unique_test_dir("move-rsync-failure");
    let config = test_config(&base);
    fs::create_dir_all(&config.movie_dir).expect("movie dir should exist");
    fs::create_dir_all(&config.tv_show_dir).expect("tv dir should exist");
    fs::create_dir_all(&config.anime_dir).expect("anime dir should exist");
    let container = base.join("downloads").join("torrents").join("abc");
    fs::create_dir_all(&container).expect("container should exist");
    let source = container.join("Show");
    fs::create_dir_all(&source).expect("payload dir should exist");

    let error = move_classified_target_with_operations(
        &config,
        &ClassificationTarget {
            source_path: source.clone(),
            cleanup_dir: Some(container.clone()),
        },
        LibraryKind::Movie,
        &|_, _| bail!("rsync exited with status 23: permission denied"),
    )
    .expect_err("external mover should fail");

    let rendered = format!("{error:#}");
    assert!(rendered.contains("failed to move"));
    assert!(rendered.contains("permission denied"));
    assert!(source.exists());
    assert!(container.exists());
    let _ = fs::remove_dir_all(&base);
}
