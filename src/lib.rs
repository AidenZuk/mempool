//! A thread-safe object pool with automatic return and attach/detach semantics
//!
//! The goal of an object pool is to reuse expensive to allocate objects or frequently allocated objects
//!
//! # Examples
//!
//! ## Creating a Pool
//!
//! The general pool creation looks like this
//! ```
//!  let pool: MemPool<T> = MemoryPool::new(capacity, || T::new());
//! ```
//! Example pool with 32 `Vec<u8>` with capacity of 4096
//! ```
//!  let pool: MemoryPool<Vec<u8>> = MemoryPool::new(32, || Vec::with_capacity(4096));
//! ```
//!
//! ## Using a Pool
//!
//! Basic usage for pulling from the pool
//! ```
//! let pool: MemoryPool<Vec<u8>> = MemoryPool::new(32, || Vec::with_capacity(4096));
//! let mut reusable_buff = pool.pull().unwrap(); // returns None when the pool is saturated
//! reusable_buff.clear(); // clear the buff before using
//! some_file.read_to_end(reusable_buff);
//! // reusable_buff is automatically returned to the pool when it goes out of scope
//! ```
//! Pull from pool and `detach()`
//! ```
//! let pool: MemoryPool<Vec<u8>> = MemoryPool::new(32, || Vec::with_capacity(4096));
//! let mut reusable_buff = pool.pull().unwrap(); // returns None when the pool is saturated
//! reusable_buff.clear(); // clear the buff before using
//! let (pool, reusable_buff) = reusable_buff.detach();
//! let mut s = String::from(reusable_buff);
//! s.push_str("hello, world!");
//! pool.attach(s.into_bytes()); // reattach the buffer before reusable goes out of scope
//! // reusable_buff is automatically returned to the pool when it goes out of scope
//! ```
//!
//! ## Using Across Threads
//!
//! You simply wrap the pool in a [`std::sync::Arc`]
//! ```
//! let pool: Arc<MemoryPool<T>> = Arc::new(MemoryPool::new(cap, || T::new()));
//! ```
//!
//! # Warning
//!
//! Objects in the pool are not automatically reset, they are returned but NOT reset
//! You may want to call `object.reset()` or  `object.clear()`
//! or any other equivalent for the object that you are using, after pulling from the pool
//!
//! [`std::sync::Arc`]: https://doc.rust-lang.org/stable/std/sync/struct.Arc.html
mod multi_buf;
mod semphore;

pub use multi_buf::{MultiBuffer, GetSegs};
use crossbeam::channel;
use std::ops::{Deref, DerefMut};
use parking_lot::{Mutex, Condvar};
use std::mem::{ManuallyDrop, forget};
use std::sync::Arc;
use std::thread;
use log::{trace};

use parking_lot::lock_api::MutexGuard;
use futures::SinkExt;
use std::thread::sleep;

pub type Stack<T> = Vec<T>;

pub struct PendingInfo<T>
    where T: Sync + Send + 'static
{
    id: String,
    notifier: channel::Sender<T>,
}

pub struct WaitingInfo<T>
    where T: Sync + Send + 'static
{
    id: String,
    //发送恢复命令
    notifier: channel::Sender<T>,
    ///最低需要多少个内存单元才能恢复
    min_request: usize,
}

pub struct MemoryPool<T>
    where T: Sync + Send + 'static
{
    objects: (channel::Sender<T>, channel::Receiver<T>),

    // the one wait for data
    pending: Arc<Mutex<Vec<PendingInfo<Reusable<T>>>>>,
    ///those who is sleeping
    waiting: Arc<Mutex<Vec<WaitingInfo<Reusable<T>>>>>,
    run_block: Arc<Mutex<()>>,
    pending_block: Arc<Mutex<()>>,
    // recycle: (channel::Sender<Reusable<'a,T>>, channel::Receiver<Reusable<'a,T>>),
}

impl<T> MemoryPool<T> where T: Sync + Send + 'static {
    #[inline]
    pub fn new<F>(cap: usize, init: F) -> MemoryPool<T>
        where
            F: Fn() -> T,
    {

        // //println!("mempool remains:{}", cap);

        let mut objects = channel::unbounded();
        for _ in 0..cap {
            &objects.0.send(init());
        }
        MemoryPool {
            objects,

            pending: Arc::new(Mutex::new(Vec::new())),
            waiting: Arc::new(Mutex::new(Vec::new())),
            run_block: Arc::new(Mutex::new(())),
            pending_block: Arc::new(Mutex::new(())),

        }
    }


    #[inline]
    pub fn len(&self) -> usize {
        self.objects.1.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.objects.1.is_empty()
    }

    #[inline]
    pub fn pending(&'static self, str: &str, sender: channel::Sender<Reusable<T>>, releasable: usize) -> (Option<Reusable<T>>, bool) {
        //println!("pending item:{}", str);
        let _x = self.pending_block.lock();
        let ret = if let Ok(item) = self.objects.1.try_recv() {
            //println!("get ok:{}", str);
            (Some(Reusable::new(&self, item)), false)
        } else if (self.pending.lock().len() == 0) {
            //println!("get should pend:{}", str);
            self.pending.lock().push(PendingInfo {
                id: String::from(str),
                notifier: sender.clone(),
            });

            (None, false)
        } else {
            let to_retry = { self.waiting.lock().len() * 15 + 2 };
            //println!("try again :{} with retries backoff:{}", str, to_retry);
            for i in 0..to_retry {
                sleep(std::time::Duration::from_secs(1));
                if let Ok(item) = self.objects.1.try_recv() {
                    //println!("get ok:{}", str);
                    return (Some(Reusable::new(&self, item)), false);
                }
            }

            //println!("get should sleep :{}", str);
            self.waiting.lock().push(WaitingInfo {
                id: String::from(str),
                notifier: sender.clone(),
                min_request: releasable,
            });
            (None, true)
        };

        ret
    }


    #[inline]
    pub fn attach(&'static self, t: T) {
        let _x = self.run_block.lock();
        //println!("attach started<<<<<<<<<<<<<<<<");

        //println!("recyled an item ");
        let mut wait_list = { self.waiting.lock() };
        //println!("check waiting list ok :{}", wait_list.len());
        if wait_list.len() > 0 && self.len() >= wait_list[0].min_request {
            //println!("remove ok<<<<<<<<<<<<<<< ");
            let item = wait_list.remove(0);
            //println!("start wakeup<<<<<<<<<<<<<<<<<<<");
            //&wait_list.remove(0);

            //println!("free cnts:{}, waking up  {}/ with min req:{} now.... ", self.len(), item.id.clone(), item.min_request);
            self.objects.0.send(t).unwrap();
            for i in 0..self.len() {
                item.notifier.send(Reusable::new(&self, self.objects.1.recv().unwrap()));
            }
            // thread::spawn(move || {
            //     item.notifier.send(()).unwrap();
            // });
        } else if self.pending.lock().len() > 0 {
            drop(wait_list);
            let pending_item = self.pending.lock().remove(0);
            //println!("fill pending:{}", pending_item.id);
            // thread::spawn(move || {
            //     pending_item.notifier.send(());
            // });
            pending_item.notifier.send(Reusable::new(&self, t));
        } else {
            drop(wait_list);

            self.objects.0.send(t).unwrap();
            //println!("push to queue:{}", self.len());
        }
    }
}


pub struct Reusable<T>
    where T: Sync + Send + 'static {
    pool: &'static MemoryPool<T>,
    data: ManuallyDrop<T>,
}

impl<T> Reusable<T>
    where T: Sync + Send + 'static {
    #[inline]
    pub fn new(pool: &'static MemoryPool<T>, t: T) -> Self {
        Self {
            pool,
            data: ManuallyDrop::new(t),
        }
    }

    // #[inline]
    // pub fn detach(mut self) -> (&'a MemoryPool<T>, T) {
    //     let ret = unsafe { (self.pool, self.take()) };
    //     forget(self);
    //     ret
    // }
    //
    unsafe fn take(&mut self) -> T {
        ManuallyDrop::take(&mut self.data)
    }
}

impl<T> Deref for Reusable<T>
    where T: Sync + Send + 'static
{
    type Target = T;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.data
    }
}

impl<T> DerefMut for Reusable<T>
    where T: Sync + Send + 'static
{
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.data
    }
}

impl<T> Drop for Reusable<T>
    where T: Sync + Send + 'static
{
    #[inline]
    fn drop(&mut self) {
        unsafe { self.pool.attach(self.take()); }
    }
}

#[cfg(test)]
mod tests {
    use crate::{MemoryPool, Reusable};
    use std::mem::drop;
    use std::ops::DerefMut;
    use std::thread;
    use std::sync::Arc;

    // #[test]
    // fn pull() {
    //     let pool = Arc::new(MemoryPool::<Vec<u8>>::new(3, || Vec::new()));
    //     let pool2 = pool.clone();
    //     let t1 = thread::spawn(move ||{
    //         let object1 = pool.lock().pull();
    //         //println!("retain 1");
    //         thread::sleep(std::time::Duration::from_secs(1));
    //
    //         let object2 = pool.pull();
    //         //println!("retain 2");
    //         thread::sleep(std::time::Duration::from_secs(1));
    //
    //         let object3 = pool.pull();
    //         //println!("retain 3");
    //         thread::sleep(std::time::Duration::from_secs(1));
    //
    //         //println!("drop 1");
    //         drop(object1);
    //         thread::sleep(std::time::Duration::from_secs(1));
    //
    //         //println!("drop 2");
    //         drop(object2);
    //         thread::sleep(std::time::Duration::from_secs(1));
    //
    //         //println!("drop 3");
    //         drop(object3);
    //         thread::sleep(std::time::Duration::from_secs(1));
    //
    //     });
    //     let t2 = thread::spawn(move ||{
    //         //println!(">>>wait for 2.5s");
    //         thread::sleep(std::time::Duration::from_millis(2500));
    //         //println!(">>>try to retain 1.....");
    //         let object2 = pool2.pull();
    //         //println!(">>>retained 1");
    //         //println!(">>>try to retain 2.....");
    //         let object2 = pool2.pull();
    //         //println!(">>>retained 1");
    //         //println!(">>>try to retain 3.....");
    //         let object2 = pool2.pull();
    //         //println!(">>>retained 1");
    //
    //         thread::sleep(std::time::Duration::from_secs(1));
    //
    //         //println!(">>>dropped");
    //         drop(object2);
    //         thread::sleep(std::time::Duration::from_secs(1));
    //
    //     });
    //     t1.join();
    //     t2.join();
    //
    // }

    #[test]
    fn e2e() {
        // let pool = MemoryPool::new(10, || Vec::new());
        // let mut objects = Vec::new();
        //
        // thread::spawn(||{
        //     for i in 0..10 {
        //         let mut object = pool.pull();
        //     }
        // });
        //
        //
        //
        // drop(objects);
        //
        //
        // for i in 10..0 {
        //     let mut object = pool.objects.lock().pop().unwrap();
        //     assert_eq!(object.pop(), Some(i));
        // }
    }
}
