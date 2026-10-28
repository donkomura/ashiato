use std::time::Duration;

use ashiato_pipeline::{
    DeliveryHandle, PartitionId, PartitionMessage, PartitionReader, PartitionWriter, Position,
    TransportError, partition_channels,
};
use tokio::time::timeout;

const SHORT_WAIT: Duration = Duration::from_millis(50);

fn partition(region: &str) -> PartitionId {
    PartitionId::new("111111111111", region)
}

fn end(partition: &PartitionId, file_seq: u64) -> PartitionMessage {
    PartitionMessage::EndOfPartition {
        partition: partition.clone(),
        position: Position::new(file_seq, 0),
    }
}

#[tokio::test]
async fn messages_of_a_partition_are_fetched_in_the_order_they_were_put() {
    let east = partition("us-east-1");
    let (writer, mut reader) = partition_channels([east.clone()], 8);
    for seq in 0..3 {
        writer.put(end(&east, seq)).await.unwrap();
    }

    let deliveries = reader.fetch(&east, 8).await.unwrap();

    let positions: Vec<Position> = deliveries
        .iter()
        .map(|delivery| delivery.message.position())
        .collect();
    assert_eq!(
        positions,
        vec![
            Position::new(0, 0),
            Position::new(1, 0),
            Position::new(2, 0)
        ]
    );
}

#[tokio::test]
async fn fetch_returns_at_most_the_requested_number_of_messages() {
    let east = partition("us-east-1");
    let (writer, mut reader) = partition_channels([east.clone()], 8);
    for seq in 0..3 {
        writer.put(end(&east, seq)).await.unwrap();
    }

    assert_eq!(reader.fetch(&east, 2).await.unwrap().len(), 2);
    assert_eq!(reader.fetch(&east, 2).await.unwrap().len(), 1);
}

#[tokio::test]
async fn deliveries_have_distinct_handles() {
    let east = partition("us-east-1");
    let (writer, mut reader) = partition_channels([east.clone()], 8);
    for seq in 0..3 {
        writer.put(end(&east, seq)).await.unwrap();
    }

    let mut handles: Vec<DeliveryHandle> = reader
        .fetch(&east, 8)
        .await
        .unwrap()
        .iter()
        .map(|delivery| delivery.handle)
        .collect();
    handles.dedup();
    assert_eq!(handles.len(), 3);
    reader.ack(&handles).await.unwrap();
}

#[tokio::test]
async fn a_full_partition_makes_its_writer_wait_without_blocking_other_partitions() {
    let east = partition("us-east-1");
    let west = partition("us-west-2");
    let (writer, mut reader) = partition_channels([east.clone(), west.clone()], 1);
    writer.put(end(&east, 0)).await.unwrap();

    let blocked = tokio::spawn({
        let writer = writer.clone();
        let east = east.clone();
        async move { writer.put(end(&east, 1)).await }
    });
    tokio::time::sleep(SHORT_WAIT).await;
    assert!(!blocked.is_finished());

    timeout(SHORT_WAIT, writer.put(end(&west, 0)))
        .await
        .expect("a put to a partition with room must not wait")
        .unwrap();

    assert_eq!(reader.fetch(&east, 1).await.unwrap().len(), 1);
    timeout(SHORT_WAIT, blocked)
        .await
        .expect("the waiting put must finish once its partition has room")
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn fetch_waits_until_a_message_arrives() {
    let east = partition("us-east-1");
    let (writer, mut reader) = partition_channels([east.clone()], 8);
    let sender = tokio::spawn({
        let east = east.clone();
        async move {
            tokio::time::sleep(SHORT_WAIT).await;
            writer.put(end(&east, 0)).await
        }
    });

    let deliveries = reader.fetch(&east, 8).await.unwrap();

    assert_eq!(deliveries.len(), 1);
    sender.await.unwrap().unwrap();
}

#[tokio::test]
async fn fetch_returns_nothing_once_all_writers_are_gone_and_the_partition_is_drained() {
    let east = partition("us-east-1");
    let (writer, mut reader) = partition_channels([east.clone()], 8);
    let last_writer = tokio::spawn({
        let east = east.clone();
        async move { writer.put(end(&east, 0)).await }
    });
    last_writer.await.unwrap().unwrap();

    assert_eq!(reader.fetch(&east, 8).await.unwrap().len(), 1);
    assert!(reader.fetch(&east, 8).await.unwrap().is_empty());
}

#[tokio::test]
async fn an_unknown_partition_is_an_error() {
    let east = partition("us-east-1");
    let west = partition("us-west-2");
    let (writer, mut reader) = partition_channels([east], 8);

    assert!(matches!(
        writer.put(end(&west, 0)).await,
        Err(TransportError::UnknownPartition(unknown)) if unknown == west
    ));
    assert!(matches!(
        reader.fetch(&west, 8).await,
        Err(TransportError::UnknownPartition(unknown)) if unknown == west
    ));
}

#[tokio::test]
async fn a_single_process_cannot_redeliver_messages() {
    let east = partition("us-east-1");
    let (_writer, mut reader) = partition_channels([east.clone()], 8);

    assert!(matches!(
        reader.reset(&east).await,
        Err(TransportError::RedeliveryUnsupported(unsupported)) if unsupported == east
    ));
}

fn positions(deliveries: &[ashiato_pipeline::Delivery]) -> Vec<Position> {
    deliveries
        .iter()
        .map(|delivery| delivery.message.position())
        .collect()
}

#[tokio::test]
async fn fetch_ready_takes_the_first_of_the_given_partitions_that_has_a_message() {
    let east = partition("us-east-1");
    let west = partition("us-west-2");
    let north = partition("eu-north-1");
    let (writer, mut reader) = partition_channels([east.clone(), west.clone(), north.clone()], 8);
    writer.put(end(&west, 0)).await.unwrap();
    writer.put(end(&west, 1)).await.unwrap();
    writer.put(end(&north, 5)).await.unwrap();

    let (index, deliveries) = reader
        .fetch_ready(&[east.clone(), north.clone(), west.clone()], 8)
        .await
        .unwrap();
    assert_eq!(index, 1);
    assert_eq!(positions(&deliveries), vec![Position::new(5, 0)]);

    let (index, deliveries) = reader
        .fetch_ready(&[east.clone(), north.clone(), west.clone()], 1)
        .await
        .unwrap();
    assert_eq!(index, 2);
    assert_eq!(positions(&deliveries), vec![Position::new(0, 0)]);
}

#[tokio::test]
async fn fetch_ready_waits_for_any_of_the_given_partitions_but_not_the_others() {
    let east = partition("us-east-1");
    let west = partition("us-west-2");
    let north = partition("eu-north-1");
    let (writer, mut reader) = partition_channels([east.clone(), west.clone(), north.clone()], 8);
    writer.put(end(&north, 0)).await.unwrap();

    assert!(
        timeout(
            SHORT_WAIT,
            reader.fetch_ready(&[east.clone(), west.clone()], 8)
        )
        .await
        .is_err(),
        "a partition that is not given must not be read"
    );
    let sender = tokio::spawn({
        let (writer, west) = (writer.clone(), west.clone());
        async move { writer.put(end(&west, 3)).await }
    });
    let (index, deliveries) = reader
        .fetch_ready(&[east.clone(), west.clone()], 8)
        .await
        .unwrap();

    assert_eq!(index, 1);
    assert_eq!(positions(&deliveries), vec![Position::new(3, 0)]);
    sender.await.unwrap().unwrap();
    assert_eq!(
        positions(&reader.fetch(&north, 8).await.unwrap()),
        vec![Position::new(0, 0)]
    );
}

#[tokio::test]
async fn fetch_ready_reports_a_drained_partition_once_all_writers_are_gone() {
    let east = partition("us-east-1");
    let west = partition("us-west-2");
    let (writer, mut reader) = partition_channels([east.clone(), west.clone()], 8);
    writer.put(end(&west, 0)).await.unwrap();
    drop(writer);

    let (index, deliveries) = reader
        .fetch_ready(&[west.clone(), east.clone()], 8)
        .await
        .unwrap();
    assert_eq!((index, deliveries.len()), (0, 1));
    let (_, deliveries) = reader
        .fetch_ready(&[west.clone(), east.clone()], 8)
        .await
        .unwrap();
    assert!(deliveries.is_empty());
}

#[tokio::test]
async fn fetch_ready_with_an_unknown_partition_is_an_error() {
    let east = partition("us-east-1");
    let west = partition("us-west-2");
    let (_writer, mut reader) = partition_channels([east.clone()], 8);

    assert!(matches!(
        reader.fetch_ready(&[east, west.clone()], 8).await,
        Err(TransportError::UnknownPartition(unknown)) if unknown == west
    ));
}
