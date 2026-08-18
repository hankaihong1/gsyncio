use pyo3::prelude::*;
use std::sync::atomic::{AtomicUsize, Ordering};

/// Each counter is 64-byte-aligned to prevent false sharing between
/// adjacent worker counters on multi-core systems.
#[repr(align(64))]
pub(crate) struct PaddedAtomic(pub(crate) AtomicUsize);

#[pyclass(module = "multiloop._multiloop_core")]
pub struct AtomicMetrics {
    active: Vec<PaddedAtomic>,
    completed: Vec<PaddedAtomic>,
    global_pull_count: Vec<PaddedAtomic>,
    park_count: Vec<PaddedAtomic>,
    injection_queue_depth: Vec<PaddedAtomic>,
    remote_schedule_count: Vec<PaddedAtomic>,
}

#[pymethods]
impl AtomicMetrics {
    #[new]
    pub fn new(num_threads: usize) -> Self {
        let mut active = Vec::with_capacity(num_threads);
        let mut completed = Vec::with_capacity(num_threads);
        let mut global_pull_count = Vec::with_capacity(num_threads);
        let mut park_count = Vec::with_capacity(num_threads);
        let mut injection_queue_depth = Vec::with_capacity(num_threads);
        let mut remote_schedule_count = Vec::with_capacity(num_threads);
        for _ in 0..num_threads {
            active.push(PaddedAtomic(AtomicUsize::new(0)));
            completed.push(PaddedAtomic(AtomicUsize::new(0)));
            global_pull_count.push(PaddedAtomic(AtomicUsize::new(0)));
            park_count.push(PaddedAtomic(AtomicUsize::new(0)));
            injection_queue_depth.push(PaddedAtomic(AtomicUsize::new(0)));
            remote_schedule_count.push(PaddedAtomic(AtomicUsize::new(0)));
        }
        AtomicMetrics {
            active,
            completed,
            global_pull_count,
            park_count,
            injection_queue_depth,
            remote_schedule_count,
        }
    }

    pub fn inc_active(&self, index: usize) {
        if index < self.active.len() {
            self.active[index].0.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn dec_active(&self, index: usize) {
        if index < self.active.len() {
            self.active[index].0.fetch_sub(1, Ordering::Relaxed);
            self.completed[index].0.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn get_active(&self, index: usize) -> usize {
        if index < self.active.len() {
            self.active[index].0.load(Ordering::Relaxed)
        } else {
            0
        }
    }

    pub fn get_completed(&self, index: usize) -> usize {
        if index < self.completed.len() {
            self.completed[index].0.load(Ordering::Relaxed)
        } else {
            0
        }
    }

    pub fn inc_global_pull(&self, index: usize) {
        if index < self.global_pull_count.len() {
            self.global_pull_count[index]
                .0
                .fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn get_global_pull(&self, index: usize) -> usize {
        if index < self.global_pull_count.len() {
            self.global_pull_count[index].0.load(Ordering::Relaxed)
        } else {
            0
        }
    }

    pub fn inc_park(&self, index: usize) {
        if index < self.park_count.len() {
            self.park_count[index].0.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn get_park(&self, index: usize) -> usize {
        if index < self.park_count.len() {
            self.park_count[index].0.load(Ordering::Relaxed)
        } else {
            0
        }
    }

    pub fn set_injection_queue_depth(&self, index: usize, depth: usize) {
        if index < self.injection_queue_depth.len() {
            self.injection_queue_depth[index]
                .0
                .store(depth, Ordering::Relaxed);
        }
    }

    pub fn get_injection_queue_depth(&self, index: usize) -> usize {
        if index < self.injection_queue_depth.len() {
            self.injection_queue_depth[index].0.load(Ordering::Relaxed)
        } else {
            0
        }
    }

    pub fn inc_remote_schedule(&self, index: usize) {
        if index < self.remote_schedule_count.len() {
            self.remote_schedule_count[index]
                .0
                .fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn get_remote_schedule(&self, index: usize) -> usize {
        if index < self.remote_schedule_count.len() {
            self.remote_schedule_count[index].0.load(Ordering::Relaxed)
        } else {
            0
        }
    }
}
