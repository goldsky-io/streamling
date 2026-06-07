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
/// Returns the number of successful sends. Receivers are only removed if a SourceComplete message is received.
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
                Err(e) => {
                    // Only remove receivers if this source has been marked as completed
                    if is_completed {
                        disconnected_ids.push(*subscriber_id);
                    } else {
                        // For sources not yet completed, raise the error
                        return Err(e);
                    }
                }
            }
        }

        // Remove disconnected senders
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
