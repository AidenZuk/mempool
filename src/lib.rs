
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
mod multi_buf ;

pub use multi_buf::{MultiBuffer,GetSegs};
use crossbeam::channel;
use std::ops::{Deref, DerefMut};
use parking_lot::{Mutex,Condvar};
use std::mem::{ManuallyDrop, forget};
use std::sync::Arc;
use std::thread;


use parking_lot::lock_api::MutexGuard;

pub type Stack<T> = Vec<T>;

pub struct PendingInfo{
    id:String,
    notifier:channel::Sender<()>,
}

pub struct WaitingInfo{
    id:String,
    //发送恢复命令
    notifier:channel::Sender<()>,
    ///最低需要多少个内存单元才能恢复
    min_request:usize,
}
pub struct MemoryPool<T> {
    objects:Arc<Mutex<Stack<T>>>,
    resources:(channel::Sender<()>,channel::Receiver<()>),
    run_block:Arc<Mutex<bool>>,
    // the one wait for data
    pending:Arc<Mutex<Vec<PendingInfo>>>,
    ///those who is sleeping
    waiting:Arc<Mutex<Vec<WaitingInfo>>>,

}

impl<T> MemoryPool<T> {
    #[inline]
    pub fn new<F>(cap: usize, init: F) -> MemoryPool<T>
        where
            F: Fn() -> T,
    {
        let mut objects = Stack::new();

        for _ in 0..cap {
            objects.push(init());
        }
       // println!("mempool remains:{}", cap);

        MemoryPool {
            objects: Arc::new(Mutex::new(objects)),
            run_block:Arc::new(Mutex::new(false)),
            resources: {
                let res = channel::unbounded();
                res.0.send(());
                res
            },
            pending:Arc::new(Mutex::new(Vec::new())),
            waiting:Arc::new(Mutex::new(Vec::new())),
        }
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.objects.lock().len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.objects.lock().is_empty()
    }

    #[inline]
    pub fn pending(&self,str:&str,sender:channel::Sender<()>) ->bool {
       let _x = self.run_block.lock();
       let can_pending = self.pending.lock().len() == 0;
        if can_pending {
            self.pending.lock().push(PendingInfo{
                id:String::from(str),
                notifier:sender.clone()
            });
        }
        can_pending
    }
    #[inline]
    pub fn start_sleep(&self,id:String,min_req:usize,notifier:channel::Sender<()>){
        let _x = self.run_block.lock();
        self.waiting.lock().push(WaitingInfo{
            id:id.clone(),
            notifier,
            min_request:min_req
        })
    }

    fn start_wakeup(&self){
        self.resources.1.recv();
        println!("start wakeup");
    }

    pub fn end_wakeup(&self) {
        self.resources.0.send(());
        println!("end wakeup");
    }
    pub fn get_item_no_lock(&self)->Option<Reusable<T>> {
        println!("lock....");
        let _x = self.run_block.lock();
        println!("lock over....");
        if let Some(item) = self.objects.lock().pop() {
            println!("objects lock. over...");
            Some(Reusable::new(self,item))
        }else{
            println!("objects lock. no item...");
            None
        }
    }
    pub fn get_item(&self) ->Option<Reusable<T>> {
        self.resources.1.recv();
        let val = self.get_item_no_lock();
        self.resources.0.send(());
        val
    }


    #[inline]
    pub fn attach(&self, t: T) {
        //let _x = self.run_block.lock();
        println!("attach started<<<<<<<<<<<<<<<<>>>>>>>>>>>>>>>>");
        self.start_wakeup();
        { self.objects.lock().push(t); }
        println!("recyled an item ");
        let mut wait_list = self.waiting.lock();
        println!("check waiting list ok ");
        if wait_list.len() > 0 {
            if self.len() >= wait_list[0].min_request {
                println!("remove ok<<<<<<<<<<<<<<<,, ");
                let item = wait_list.remove(0);
                println!("start wakeup<<<<<<<<<<<<<<<<<<<");
                //&wait_list.remove(0);

                println!("free cnts:{}, waking up  {}/ with min req:{} now.... ",self.len(),item.id.clone(),item.min_request);

                thread::spawn(move||{
                    item.notifier.send(()).unwrap();
                });
            }else{
                self.end_wakeup();
            }
        }else {
            println!("pending data ");
            if let Some(pending_item) = self.pending.lock().pop() {
                thread::spawn(move|| {
                    pending_item.notifier.send(());
                });
            }
            self.end_wakeup();
        }
        println!("attach finished<<<<<<<<<<<<<<<<>>>>>>>>>>>>>>>>");
    }
}

pub struct Reusable<'a, T> {
    pool: &'a MemoryPool<T>,
    data: ManuallyDrop<T>,
}

impl<'a, T> Reusable<'a, T> {
    #[inline]
    pub fn new(pool: &'a MemoryPool<T>, t: T) -> Self {
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

impl<'a, T> Deref for Reusable<'a, T> {
    type Target = T;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.data
    }
}

impl<'a, T> DerefMut for Reusable<'a, T> {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.data
    }
}

impl<'a, T> Drop for Reusable<'a, T> {
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
    //         println!("retain 1");
    //         thread::sleep(std::time::Duration::from_secs(1));
    //
    //         let object2 = pool.pull();
    //         println!("retain 2");
    //         thread::sleep(std::time::Duration::from_secs(1));
    //
    //         let object3 = pool.pull();
    //         println!("retain 3");
    //         thread::sleep(std::time::Duration::from_secs(1));
    //
    //         println!("drop 1");
    //         drop(object1);
    //         thread::sleep(std::time::Duration::from_secs(1));
    //
    //         println!("drop 2");
    //         drop(object2);
    //         thread::sleep(std::time::Duration::from_secs(1));
    //
    //         println!("drop 3");
    //         drop(object3);
    //         thread::sleep(std::time::Duration::from_secs(1));
    //
    //     });
    //     let t2 = thread::spawn(move ||{
    //         println!(">>>wait for 2.5s");
    //         thread::sleep(std::time::Duration::from_millis(2500));
    //         println!(">>>try to retain 1.....");
    //         let object2 = pool2.pull();
    //         println!(">>>retained 1");
    //         println!(">>>try to retain 2.....");
    //         let object2 = pool2.pull();
    //         println!(">>>retained 1");
    //         println!(">>>try to retain 3.....");
    //         let object2 = pool2.pull();
    //         println!(">>>retained 1");
    //
    //         thread::sleep(std::time::Duration::from_secs(1));
    //
    //         println!(">>>dropped");
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
