//! This module contains the implementation of the broadcast channels used to exchange
//! checkpoint messages between different operators in the topology.

use crate::checkpoints::checkpoint_management::CheckpointMessage;
use crossbeam::channel;
use once_cell::sync::Lazy;
use parking_lot::RwLock;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

/// Unique identifier for a subscriber
pub type SubscriberId = u64;

static NEXT_SUBSCRIBER_ID: AtomicU64 = AtomicU64::new(0);

pub struct BroadcastChannels<T> {
    senders: HashMap<String, HashMap<SubscriberId, channel::Sender<T>>>,
    completed_sources: HashSet<String>,
}

impl<T: 'static + Clone + Send + Sync> Default for BroadcastChannels<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: 'static + Clone + Send + Sync> BroadcastChannels<T> {
    pub fn new() -> Self {
        Self {
            senders: HashMap::new(),
            completed_sources: HashSet::new(),
        }
    }
}

static CHECKPOINT_CHANNELS: Lazy<Arc<RwLock<BroadcastChannels<CheckpointMessage>>>> =
    Lazy::new(|| Arc::new(RwLock::new(BroadcastChannels::new())));

/// Subscribe to a checkpoint channel using the reference name of the operator.
pub fn subscribe(id: &str) -> channel::Receiver<CheckpointMessage> {
    let (rx, _) = subscribe_with_id(id);
    rx
}

/// Subscribe to a checkpoint channel using the reference name of the operator.
/// Returns a tuple of (receiver, subscriber_id) where subscriber_id can be used to unsubscribe.
pub fn subscribe_with_id(id: &str) -> (channel::Receiver<CheckpointMessage>, SubscriberId) {
    let mut channels = CHECKPOINT_CHANNELS.write();

    let (tx, rx) = channel::unbounded();
    let subscriber_id = NEXT_SUBSCRIBER_ID.fetch_add(1, Ordering::SeqCst);

    if let Some(senders) = channels.senders.get_mut(id) {
        senders.insert(subscriber_id, tx);
    } else {
        let mut map = HashMap::new();
        map.insert(subscriber_id, tx);
        channels.senders.insert(id.to_string(), map);
    }

    (rx, subscriber_id)
}

/// Unsubscribe from a checkpoint channel. This should be called before dropping the receiver
/// to avoid SendError when other senders try to broadcast to this channel.
pub fn unsubscribe(id: &str, subscriber_id: SubscriberId) {
    let mut channels = CHECKPOINT_CHANNELS.write();

    if let Some(senders) = channels.senders.get_mut(id) {
        senders.remove(&subscriber_id);
    }
}

/// Send a checkpoint message to all subscribers of the checkpoint channel using the reference name of the operator.
/// Returns the number of successful sends.
///
/// The broadcast always attempts EVERY subscriber: a dead one (receiver
/// dropped without `unsubscribe`) is pruned and the remaining live subscribers
/// still receive the message. This used to early-return on the first dead
/// subscriber for non-completed sources, which silently starved every
/// subscriber later in the (arbitrary) iteration order of Markers/Acks/
/// Finalizers the moment any component exited uncleanly during shutdown.
/// Dead subscribers on a source not marked complete are logged, since they
/// indicate a component that dropped its receiver without unsubscribing.
pub fn send(
    id: &str,
    message: CheckpointMessage,
) -> Result<usize, channel::SendError<CheckpointMessage>> {
    let mut channels = CHECKPOINT_CHANNELS.write();
    let mut successful_sends = 0;

    // Handle SourceComplete message - mark source as completed and allow cleanup
    if let CheckpointMessage::SourceComplete(source_name) = &message {
        channels.completed_sources.insert(source_name.clone());
    }

    let is_completed = channels.completed_sources.contains(id);

    if let Some(senders) = channels.senders.get_mut(id) {
        let mut disconnected_ids = Vec::new();

        for (subscriber_id, sender) in senders.iter() {
            match sender.send(message.clone()) {
                Ok(()) => successful_sends += 1,
                Err(_) => disconnected_ids.push(*subscriber_id),
            }
        }

        // Prune disconnected senders: a dropped crossbeam receiver never comes
        // back, so keeping the sender only makes every future broadcast fail
        // against it.
        if !disconnected_ids.is_empty() && !is_completed {
            tracing::warn!(
                "checkpoint channel '{}': pruned {} disconnected subscriber(s) mid-broadcast \
                 (a component dropped its receiver without unsubscribing)",
                id,
                disconnected_ids.len()
            );
        }
        for subscriber_id in disconnected_ids {
            senders.remove(&subscriber_id);
        }

        // Clean up empty sender lists only if source is completed
        if senders.is_empty() && channels.completed_sources.contains(id) {
            channels.senders.remove(id);
            channels.completed_sources.remove(id);
        }
    }

    Ok(successful_sends)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::checkpoints::checkpoint_management::{CheckpointEpoch, CheckpointMessage};

    #[test]
    fn send_delivers_to_all_live_subscribers_despite_a_dead_one() {
        // Three subscribers; the middle one drops its receiver WITHOUT
        // unsubscribing (an unclean component exit). The broadcast must still
        // deliver to both live subscribers — the old behaviour early-returned
        // on the first dead sender it happened to iterate, silently starving
        // whoever came after it.
        let channel_id = "channels_test_partial_delivery";
        let (rx_a, _id_a) = subscribe_with_id(channel_id);
        let (rx_dead, _id_dead) = subscribe_with_id(channel_id);
        let (rx_b, _id_b) = subscribe_with_id(channel_id);
        drop(rx_dead);

        let sent = send(
            channel_id,
            CheckpointMessage::Finalizer(CheckpointEpoch(99)),
        )
        .expect("broadcast must not fail because one subscriber died");
        assert_eq!(sent, 2, "both live subscribers must receive the message");
        assert!(matches!(
            rx_a.try_recv(),
            Ok(CheckpointMessage::Finalizer(CheckpointEpoch(99)))
        ));
        assert!(matches!(
            rx_b.try_recv(),
            Ok(CheckpointMessage::Finalizer(CheckpointEpoch(99)))
        ));

        // The dead subscriber was pruned: a second send reaches the same two.
        let sent = send(
            channel_id,
            CheckpointMessage::Finalizer(CheckpointEpoch(100)),
        )
        .expect("broadcast must not fail after pruning");
        assert_eq!(sent, 2);
    }
}
