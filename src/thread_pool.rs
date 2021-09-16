use std::thread::JoinHandle;
use std::sync::atomic::{AtomicBool, Ordering};
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
            std::mem::transmute::<Box<dyn FnOnce() + Send + 'scope>, Box<dyn FnOnce() + Send + 'static>>(Box::new(task))
        };

        self.work_queue_sender.send(task)
            .map_err(|_| ThreadPoolError::SpawnError)
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
}
