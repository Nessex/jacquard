use std::sync::{Mutex, Arc};
use std::collections::VecDeque;
use std::thread::JoinHandle;
use std::sync::atomic::{AtomicBool, Ordering};

pub struct ThreadPool
{
    done: Arc<AtomicBool>,
    work_queue: Arc<Mutex<VecDeque<Box<dyn 'static + FnOnce() + Send>>>>,
    handles: Vec<JoinHandle<()>>,
}

impl ThreadPool {
    pub fn new() -> Self {
        let mut s = Self {
            done: Arc::new(AtomicBool::new(false)),
            work_queue: Arc::new(Mutex::new(VecDeque::new())),
            handles: Vec::new(),
        };

        s.run();

        s
    }

    fn run(&mut self) {
        for _ in 0..num_cpus::get() {
            self.handles.push(self.run_thread());
        }
    }

    fn run_thread(&self) -> JoinHandle<()> {
        let done = self.done.clone();
        let work_queue = self.work_queue.clone();

        std::thread::spawn(move || {
            loop {
                let mut g = match work_queue.try_lock() {
                    Ok(g) => g,
                    Err(_) => {
                        continue;
                    },
                };

                let task = g.pop_front();
                drop(g);

                if let Some(task) = task {
                    task();
                } else if done.load(Ordering::Acquire) {
                    break;
                } else {
                    std::thread::yield_now();
                }
            }
        })
    }

    pub fn spawn<F>(&self, task: F)
        where
            F: 'static + FnOnce() + Send,
    {
        let mut q = self.work_queue.lock().unwrap();

        q.push_back(Box::new(task));
    }

    fn join_all(&mut self) {
        for h in self.handles.drain(..) {
            h.join().unwrap();
        }
    }
}

impl Drop for ThreadPool {
    fn drop(&mut self) {
        self.done.fetch_or(true, Ordering::Release);
        self.join_all();
    }
}

#[cfg(test)]
mod tests {
    use crate::ThreadPool;
    use std::io::Write;
    use std::time::{Duration, Instant};
    use rayon::prelude::*;

    #[test]
    fn time_jacquard() {
        let start = Instant::now();
        let tp = ThreadPool::new();
        let t = std::thread::current().id();
        for _ in 0..1_000_000 {
            tp.spawn(move || {
                let thread_id = std::thread::current().id();

                assert_ne!(t, thread_id);
            });
        }
        drop(tp);
        println!("jcq: {}ms", start.elapsed().as_millis());
    }

    #[test]
    fn time_rayon() {
        let start = Instant::now();
        let tp = rayon::ThreadPoolBuilder::new().build().unwrap();
        let t = std::thread::current().id();
        tp.scope(|s| {
            for _ in 0..1_000_000 {
                s.spawn(move |_| {
                    let thread_id = std::thread::current().id();

                    assert_ne!(t, thread_id);
                });
            }
        });
        drop(tp);
        println!("rayon: {}ms", start.elapsed().as_millis());
    }
}
