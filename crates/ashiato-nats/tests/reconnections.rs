use std::sync::Arc;
use std::sync::atomic::Ordering;

use ashiato_nats::Reconnections;
use async_nats::Statistics;

fn statistics(connects: u64) -> Arc<Statistics> {
    let statistics = Arc::new(Statistics::default());
    statistics.connects.store(connects, Ordering::Relaxed);
    statistics
}

#[test]
fn the_connection_at_the_start_is_not_a_reconnection() {
    let mut reconnections = Reconnections::new(statistics(1));

    assert!(!reconnections.since_last_check());
}

#[test]
fn a_reconnection_is_reported_once_at_the_next_check() {
    let statistics = statistics(1);
    let mut reconnections = Reconnections::new(Arc::clone(&statistics));

    statistics.connects.fetch_add(1, Ordering::Relaxed);

    assert!(reconnections.since_last_check());
    assert!(!reconnections.since_last_check());
}

#[test]
fn several_reconnections_between_two_checks_are_reported_once() {
    let statistics = statistics(1);
    let mut reconnections = Reconnections::new(Arc::clone(&statistics));
    assert!(!reconnections.since_last_check());

    statistics.connects.fetch_add(3, Ordering::Relaxed);

    assert!(reconnections.since_last_check());
    assert!(!reconnections.since_last_check());
    statistics.connects.fetch_add(1, Ordering::Relaxed);
    assert!(reconnections.since_last_check());
}
