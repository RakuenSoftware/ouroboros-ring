//! Delivery of subscribed notifications and overflow to a process's handlers
//! (D14), on one thread, in the order the ring delivered them, so a slow
//! handler delays the next notification and never the ring.

use crate::client::Event;
use crate::layout::{OVERFLOW_LEN, OVERFLOW_OFF_SHED_KIND};
use crate::wire::{F_CONTROL, F_NOTIFICATION, KIND_MODULE_BASE, KIND_OVERFLOW};
use std::collections::{BTreeSet, VecDeque};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::JoinHandle;

/// A notification handler: the kind and a copy of the payload.
pub type NotifyHandler = Arc<dyn Fn(u32, &[u8]) + Send + Sync>;
/// An overflow handler: the kind of a notification this slot missed.
pub type OverflowHandler = Arc<dyn Fn(u32) + Send + Sync>;

/// Notifications accepted but not yet handled. Past it a notification is
/// dropped and its kind reported as missed, as the host does for a full ring
/// (D6): the subscriber is told, and reconciles, instead of the process
/// growing without bound.
pub(crate) const NOTIFY_QUEUE_MAX: usize = 4096;

#[derive(Default)]
struct State {
    queue: VecDeque<(u32, Vec<u8>)>,
    /// The batch taken from `queue` and not yet handled, so the bound covers
    /// everything accepted, not only what is still waiting.
    inflight: usize,
    missed: BTreeSet<u32>,
    closed: bool,
}

struct Shared {
    state: Mutex<State>,
    ready: Condvar,
}

pub(crate) struct Notifier {
    shared: Arc<Shared>,
    thread: Option<JoinHandle<()>>,
    has_notify: bool,
    has_overflow: bool,
}

impl Notifier {
    pub(crate) fn new(
        notify: Option<NotifyHandler>,
        overflow: Option<OverflowHandler>,
    ) -> Notifier {
        let shared = Arc::new(Shared {
            state: Mutex::new(State::default()),
            ready: Condvar::new(),
        });
        let (has_notify, has_overflow) = (notify.is_some(), overflow.is_some());
        let worker = Arc::clone(&shared);
        let thread = std::thread::Builder::new()
            .name("ouroboros-ring-notify".into())
            .spawn(move || run(&worker, notify, overflow))
            .ok();
        Notifier {
            shared,
            thread,
            has_notify,
            has_overflow,
        }
    }

    /// Take a polled event if it is a notification or an overflow, and
    /// report whether it did.
    pub(crate) fn offer(&self, event: &Event) -> bool {
        let f = &event.frame;
        if f.hdr_flags & F_NOTIFICATION == 0 {
            return false;
        }
        let mut st = self.shared.state.lock().unwrap_or_else(|p| p.into_inner());
        if f.hdr_flags & F_CONTROL != 0 {
            if f.event_kind != KIND_OVERFLOW {
                return false;
            }
            if self.has_overflow && !st.closed && event.payload.len() >= OVERFLOW_LEN {
                let k = &event.payload[OVERFLOW_OFF_SHED_KIND..OVERFLOW_OFF_SHED_KIND + 4];
                st.missed
                    .insert(u32::from_le_bytes([k[0], k[1], k[2], k[3]]));
                self.shared.ready.notify_one();
            }
            return true;
        }
        if f.event_kind < KIND_MODULE_BASE {
            return false;
        }
        if self.has_notify && !st.closed {
            if st.queue.len() + st.inflight >= NOTIFY_QUEUE_MAX {
                st.missed.insert(f.event_kind);
            } else {
                st.queue.push_back((f.event_kind, event.payload.clone()));
            }
            self.shared.ready.notify_one();
        }
        true
    }
}

impl Drop for Notifier {
    fn drop(&mut self) {
        {
            let mut st = self.shared.state.lock().unwrap_or_else(|p| p.into_inner());
            st.closed = true;
            self.shared.ready.notify_one();
        }
        if let Some(t) = self.thread.take() {
            let _ = t.join();
        }
    }
}

fn run(shared: &Shared, notify: Option<NotifyHandler>, overflow: Option<OverflowHandler>) {
    loop {
        let (batch, missed) = {
            let mut st = shared.state.lock().unwrap_or_else(|p| p.into_inner());
            while !st.closed && st.queue.is_empty() && st.missed.is_empty() {
                st = shared.ready.wait(st).unwrap_or_else(|p| p.into_inner());
            }
            if st.closed {
                return;
            }
            let batch = std::mem::take(&mut st.queue);
            st.inflight = batch.len();
            (batch, std::mem::take(&mut st.missed))
        };
        for (kind, payload) in batch {
            if let Some(f) = &notify {
                f(kind, &payload);
            }
            let mut st = shared.state.lock().unwrap_or_else(|p| p.into_inner());
            st.inflight -= 1;
        }
        if let Some(f) = &overflow {
            for kind in missed {
                f(kind);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wire::{F_INLINE, F_REPLY, F_REQUEST, Frame, KIND_CAPABILITY_DENIED};
    use std::sync::mpsc;

    fn note(kind: u32, flags: u16) -> Event {
        Event {
            frame: Frame {
                hdr_flags: flags | F_INLINE,
                event_kind: kind,
                payload_len: 1,
                ..Frame::default()
            },
            payload: vec![b'x'],
        }
    }

    #[test]
    fn a_full_queue_reports_the_kind_it_dropped() {
        let (release_tx, release_rx) = mpsc::channel::<()>();
        let release = Mutex::new(release_rx);
        let delivered = Arc::new(Mutex::new(0usize));
        let missed = Arc::new(Mutex::new(Vec::new()));
        let (d, m) = (Arc::clone(&delivered), Arc::clone(&missed));
        let n = Notifier::new(
            Some(Arc::new(move |_, _: &[u8]| {
                let _ = release.lock().unwrap().recv();
                *d.lock().unwrap() += 1;
            })),
            Some(Arc::new(move |k| m.lock().unwrap().push(k))),
        );
        let total = NOTIFY_QUEUE_MAX + 50;
        for _ in 0..total {
            assert!(n.offer(&note(777, F_NOTIFICATION)));
        }
        drop(release_tx); // every blocked handler returns at once
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while missed.lock().unwrap().is_empty() {
            assert!(std::time::Instant::now() < deadline, "no overflow reported");
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
        drop(n);
        let got = *delivered.lock().unwrap();
        assert!(
            (NOTIFY_QUEUE_MAX..total).contains(&got),
            "delivered {got} of {total}"
        );
        assert_eq!(*missed.lock().unwrap(), vec![777]);
    }

    #[test]
    fn replies_requests_and_other_control_frames_are_left_alone() {
        let n = Notifier::new(Some(Arc::new(|_, _: &[u8]| {})), None);
        for ev in [
            note(900, F_REPLY),
            note(900, F_REQUEST),
            note(KIND_CAPABILITY_DENIED, F_NOTIFICATION | F_CONTROL),
            note(7, F_NOTIFICATION),
        ] {
            assert!(!n.offer(&ev), "took {:?}", ev.frame);
        }
    }
}
