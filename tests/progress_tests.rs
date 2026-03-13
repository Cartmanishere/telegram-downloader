use telegram_downloader_rust::progress::ProgressTracker;

#[tokio::test]
async fn absolute_progress_switches_to_known_total() {
    let tracker = ProgressTracker::new("sample.bin".to_string(), None);
    tracker
        .set_absolute_progress(5 * 1024 * 1024, None)
        .await
        .expect("absolute progress should render");
    let initial = tracker.current_status_text().await;
    assert!(initial.contains("MB |"));
    assert!(!initial.contains('%'));

    tracker
        .set_absolute_progress(5 * 1024 * 1024, Some(10 * 1024 * 1024))
        .await
        .expect("absolute progress with total should render");
    let updated = tracker.current_status_text().await;
    assert!(updated.contains("50.0%"));
    assert!(updated.contains("ETA"));
}

#[tokio::test]
async fn external_progress_includes_current_item_label_and_speed() {
    let tracker = ProgressTracker::new("mega_job".to_string(), None);
    tracker
        .set_external_progress(
            "episode01.mkv".to_string(),
            512 * 1024,
            Some(1024 * 1024),
            Some(2.5),
        )
        .await
        .expect("external progress should render");

    let status = tracker.current_status_text().await;
    assert!(status.contains("episode01.mkv"));
    assert!(status.contains("50.0%"));
    assert!(status.contains("2.50 MB/s"));
}
