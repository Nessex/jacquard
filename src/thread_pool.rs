use std::thread::JoinHandle;
use std::sync::atomic::{AtomicBool, Ordering, AtomicUsize};
use crossbeam_channel::{Sender, Receiver};
use std::sync::Arc;
use std::marker::PhantomData;

#[derive(Debug)]
pub enum ThreadPoolError {
    SpawnError,
}

pub struct ThreadPool<'scope>
{
    done: Arc<AtomicBool>,
    work_queue_sender: Sender<Box<dyn 'static + FnOnce() + Send>>,
    work_queue_receiver: Receiver<Box<dyn 'static + FnOnce() + Send>>,
    handles: Vec<JoinHandle<()>>,
    _phantom: PhantomData<&'scope ()>,
}

pub struct ThreadPoolScope<'sscope, 'tpscope> {
    pool: &'tpscope ThreadPool<'tpscope>,
    queued_jobs: AtomicUsize,
    _phantom: PhantomData<&'sscope ()>,
}

impl<'scope> ThreadPool<'scope> {
    pub fn new() -> Self {
        let (sender, receiver) = crossbeam_channel::unbounded();
        let mut s = Self {
            done: Arc::new(AtomicBool::new(false)),
            work_queue_sender: sender,
            work_queue_receiver: receiver,
            handles: Vec::new(),
            _phantom: PhantomData::default(),
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
        let receiver = self.work_queue_receiver.clone();

        std::thread::spawn(move || {
            loop {
                let task = receiver.try_recv();

                if let Ok(task) = task {
                    task();
                } else if done.load(Ordering::Acquire) {
                    break;
                } else {
                    std::thread::yield_now();
                }
            }
        })
    }

    pub fn spawn<F>(&self, task: F) -> Result<(), ThreadPoolError>
    where
        F: 'scope + FnOnce() + Send,
    {
        let task = unsafe {
            // Safety: All tasks in the pool will be executed before returning from each thread.
            // and all threads will be joined before fully dropping ThreadPool to ensure
            // that all tasks have been run to completion.
            std::mem::transmute::<Box<dyn FnOnce() + Send + 'scope>, Box<dyn FnOnce() + Send + 'static>>(Box::new(task))
        };

        self.work_queue_sender.send(task)
            .map_err(|_| ThreadPoolError::SpawnError)
    }

    pub fn scope<'sscope, F>(&self, closure: F)
    where
        F: 'sscope + 'scope + FnOnce(&ThreadPoolScope) + Send,
    {
        closure(&ThreadPoolScope {
            pool: self,
            queued_jobs: Default::default(),
            _phantom: Default::default()
        })
    }

    #[cfg(test)]
    #[doc(hidden)]
    pub fn queue_len(&self) -> usize {
        self.work_queue_receiver.len()
    }

    fn join_all(&mut self) {
        for h in self.handles.drain(..) {
            h.join().unwrap();
        }
    }
}

impl Drop for ThreadPool<'_> {
    fn drop(&mut self) {
        self.done.fetch_or(true, Ordering::Release);
        self.join_all();
    }
}

impl<'sscope, 'tpscope> ThreadPoolScope<'sscope, 'tpscope> {
    pub fn spawn<F>(&self, task: F) -> Result<(), ThreadPoolError>
        where
            F: 'sscope + 'tpscope + FnOnce(&ThreadPoolScope) + Send,
    {
        let scope = self;
        self.queued_jobs.fetch_add(1, Ordering::AcqRel);
        self.pool.spawn(move || {
            task(scope);
            self.queued_jobs.fetch_sub(1, Ordering::AcqRel);
        })
    }

    fn join_all(&self) {
        while self.queued_jobs.load(Ordering::Acquire) != 0 {
            std::thread::yield_now();
        }
    }
}

impl<'sscope, 'tpscope> Drop for ThreadPoolScope<'sscope, 'tpscope> {
    fn drop(&mut self) {
        self.join_all();
    }
}

#[cfg(test)]
mod tests {
    use crate::ThreadPool;
    use std::time::Instant;

    #[test]
    fn time_jacquard() {
        let start = Instant::now();
        let tp = ThreadPool::new();
        let t = std::thread::current().id();
        for _ in 0..1_000_000 {
            tp.spawn(move || {
                let thread_id = std::thread::current().id();

                assert_ne!(t, thread_id);
            }).unwrap();
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

    #[test]
    fn test_scope() {
        let tp = ThreadPool::new();
        let t = std::thread::current().id();
        tp.scope(|s| {
            for _ in 0..500_000 {
                s.spawn(move |_| {
                    let thread_id = std::thread::current().id();

                    assert_ne!(t, thread_id);
                }).unwrap();
            }
        });

        assert_eq!(tp.queue_len(), 0);

        tp.scope(|s| {
            for _ in 0..500_000 {
                s.spawn(move |_| {
                    let thread_id = std::thread::current().id();

                    assert_ne!(t, thread_id);
                }).unwrap();
            }
        });

        drop(tp);
    }
}
