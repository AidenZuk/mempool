use std::{vec, fs};
use anyhow::{*, Result};

use std::env;
use std::thread;
use crate::{Reusable,MemoryPool};
use crossbeam::crossbeam_channel;
const ENV_MEMORY_CACHE: &str = "FIL_PROOFS_MEMORY_CACHE_PATH";

pub struct MultiBuffer<'a> {
    seg_exp: u8,
    seg_len: usize,
    seg_len_mask:usize,
    //fast calculation
    buffers: Vec<Reusable<'a,Vec<u8>>>,
    total_len: usize,
    mem_pool:&'a MemoryPool<Vec<u8>>,
    id:String,
    store_files:Vec<PathBuf>,
    path_parent:PathBuf
}

// use storage_proofs_core::{
//     util::{data_at_node_offset, NODE_SIZE},
// };
use std::thread::sleep;
use std::sync::mpsc::channel;
use std::ops::{Deref, DerefMut};
use std::fs::OpenOptions;
use std::io::Read;
use std::path::PathBuf;
use std::process::Command;

impl<'a> MultiBuffer<'a> {
    pub fn new(seg_exp: u8,id:&str,mem_pool:&'a MemoryPool<Vec<u8>>) -> Self {

        let seg_len = (1 << seg_exp);
        let path_parent = if let Ok(path_cache) = env::var(ENV_MEMORY_CACHE){
            PathBuf::from(path_cache).join(id)
        }else if let Ok(path_cache) = env::var("TMPDIR"){
            PathBuf::from(path_cache).join(id)
        }else{
            PathBuf::from("/var/tmp").join(id)
        };
        Command::new("mkdir")
            .arg("-p")
            .arg(&path_parent)
            .output()
            .expect("failed to create cache path");
        MultiBuffer {
            seg_exp,
            seg_len,
            seg_len_mask:seg_len-1,
            buffers: Vec::new(),
            total_len: 0,
            mem_pool,
            id:String::from(id),
            path_parent,
            store_files:Vec::new(),
        }
    }
    pub fn copy_to_slice(&self, target: &mut [u8]) {
        unsafe {
            for (i, buffer) in self.buffers.iter().enumerate() {
                let start = i << self.seg_exp;
                std::ptr::copy_nonoverlapping(buffer.as_ptr(), (&mut target[start..start + self.seg_len]).as_mut_ptr(), self.seg_len);
                //( &mut target[start..start+self.seg_len]).clone_from_slice(buffer)
            }
        }
    }
    pub fn read(&self, start: usize, len: usize, target: &mut [u8]) -> Result<()> {
        if (start >= self.total_len) || (start + len > self.total_len) {
            Err(anyhow!("error in data len"))
        } else {
            let mut start_seg = start >> self.seg_exp;
            let end_seg = (start + len - 1) >> self.seg_exp;
            let seg_offset = start & (self.seg_len_mask);
            unsafe {
                if start_seg == end_seg {
                    std::ptr::copy_nonoverlapping((&self.buffers[start_seg][seg_offset..seg_offset + len]).as_ptr(), target[..].as_mut_ptr(), len);
//                    (&mut target[..len]).clone_from_slice(&self.buffers[start_seg][seg_offset..seg_offset+len]);
                } else {
                    let mut copied_len = self.seg_len - seg_offset;
                    // let l1 = (&self.buffers[start_seg][seg_offset..self.seg_len]).as_ptr();
                    // let l2 = target[..copied_len].as_mut_ptr();
                    std::ptr::copy_nonoverlapping((&self.buffers[start_seg][seg_offset..self.seg_len]).as_ptr(), target[..].as_mut_ptr(), copied_len);
                    //(&mut target[..copied_len]).clone_from_slice(&self.buffers[start_seg][seg_offset..self.seg_len]);
                    start_seg += 1;
                    while start_seg < end_seg {
                        std::ptr::copy_nonoverlapping((&self.buffers[start_seg][..self.seg_len]).as_ptr(), target[copied_len..].as_mut_ptr(), self.seg_len);
                        //(&mut target[copied_len..copied_len+self.seg_len]).clone_from_slice(&self.buffers[start_seg][..self.seg_len]);
                        copied_len += self.seg_len;
                        start_seg += 1;
                    }
                    std::ptr::copy_nonoverlapping((&self.buffers[start_seg][..len - copied_len]).as_ptr(), target[copied_len..].as_mut_ptr(), len - copied_len);
                    //(&mut target[copied_len..len]).clone_from_slice(&self.buffers[start_seg][..len-copied_len]);
                }
            }

            Ok(())
        }
    }
    fn get_memory(&mut self){
        if let Some(item) = self.mem_pool.get_item(){
            println!("{} get an item,left:{}",self.id.clone(),self.mem_pool.len());
            self.buffers.push(item);
        }else{
            let (sender,receiver) = crossbeam_channel::unbounded();
            if self.mem_pool.pending(&self.id,sender.clone()){
                println!("{} pending an item ",self.id.clone());
                receiver.recv().unwrap();
                println!("{} pending finished ",self.id.clone());
                self.buffers.push(self.mem_pool.get_item().unwrap());
            }else{

                let min_req = self.buffers.len() + 1;
                println!("{} sleep with min_req:{} ",self.id.clone(),min_req);
                //通知mem pool 我自觉释放了
                self.mem_pool.start_sleep((&self).id.clone(),min_req,sender.clone());
                //休眠
                self.sleep();
                //等待允许启动

                let _result = receiver.recv().unwrap();
                println!("{} awake now min_req:{} ",self.id.clone(),min_req);
                //获取内存
                for i in 0..min_req {
                    self.buffers.push(self.mem_pool.get_item_no_lock().unwrap())
                }
                println!("{} waked up with min_req:{} ",self.id.clone(),min_req);
                self.mem_pool.end_wakeup();
                //恢复
                self.wakeup();
                println!("{} waked up !!!! ",self.id.clone());

            }
        }
    }
    pub fn write(&mut self, start: usize, len: usize, source: &[u8]) -> Result<()> {
        if len == 1 {
            println!("???")
        }
        if source.len() != len {
            return Err(anyhow!("error in source size:{}/{}",len,source.len()));
        }
        //prepare buffer
        if start + len >= self.total_len {
            //计算需要几个buffer
            let buff_counts = ((start + len + self.seg_len - 1) >> self.seg_exp);
            for _i in self.buffers.len()..buff_counts {
                self.get_memory();
            }
            self.total_len = self.buffers.len() << self.seg_exp;
        }

        let mut start_seg = start >> self.seg_exp;
        let end_seg = (start + len - 1) >> self.seg_exp;
        let seg_offset = start & (self.seg_len_mask);
        unsafe {
            if start_seg == end_seg {
                std::ptr::copy_nonoverlapping((&source).as_ptr(), self.buffers[start_seg][seg_offset..].as_mut_ptr(), len)
                //  &self.buffers[start_seg][seg_offset..seg_offset+len].clone_from_slice(&source[..len]);
            } else {
                let mut copied_len = self.seg_len - seg_offset;
                std::ptr::copy_nonoverlapping(source.as_ptr(), (&mut self.buffers[start_seg][seg_offset..]).as_mut_ptr(), copied_len);
                //&self.buffers[start_seg][seg_offset..self.seg_len].clone_from_slice(&source[..copied_len]);
                start_seg += 1;
                while start_seg < end_seg {
                    std::ptr::copy_nonoverlapping((&source[copied_len..]).as_ptr(), (&mut self.buffers[start_seg][..]).as_mut_ptr(), self.seg_len);
                    //&self.buffers[start_seg][..self.seg_len].clone_from_slice(&source[copied_len..copied_len+self.seg_len]);
                    copied_len += self.seg_len;
                    start_seg += 1;
                }
                std::ptr::copy_nonoverlapping((&source[copied_len..]).as_ptr(), (&mut self.buffers[start_seg]).as_mut_ptr(), len - copied_len);
                //&self.buffers[start_seg][..len-copied_len].clone_from_slice(&source[copied_len..len]);
            }
        }

        Ok(())
    }
    pub fn len(&self) -> usize {
        self.total_len
    }

    fn sleep(&mut self){
        println!("{} sleeping with min_req:{}",self.id.clone(),&self.buffers.len());
        self.store_files.clear();
        for (i,val) in self.buffers.iter().enumerate(){
            let file_name= self.path_parent.join( format!("cache_{}.dat",i));
            fs::write(&file_name,val.deref());
            self.store_files.push((&file_name).clone());
            drop(val)
        }
        drop(&self.buffers);
        self.buffers = Vec::new();
    }
    fn wakeup(&mut self){
        let total_count = self.store_files.len();
        println!("{} wake up with min_req:{}",self.id.clone(),total_count);
        for file_id in 0..total_count {
            let file_path= self.path_parent.join( format!("cache_{}.dat",file_id));
            let mut open_option = OpenOptions::new();

            let data = self.buffers[file_id].deref_mut();
            if let Ok(mut file) = open_option.read(true).open(&file_path){
                file.read_to_end(data);

                println!("remove mem cache file:{:?}",&file_path.display());
                fs::remove_file(&file_path);
            }else{
                panic!("memory cache lost:{:?}",file_path.display())
            }
        }
        self.store_files.clear();
    }
    // #[inline]
    // pub fn prefetch(&self, parents: &[u32]) {
    //     for parent in parents {
    //         let start = *parent as usize * NODE_SIZE;
    //         if self.total_len <= start {
    //             continue;
    //         }
    //
    //         let start_seg = start >> self.seg_exp;
    //         let offset = (start & self.seg_len_mask);
    //         prefetch!(self.buffers[start_seg][offset..offset + NODE_SIZE].as_ptr() as *const i8);
    //     }
    // }
}


#[cfg(test)]
mod Tests {
    use crate::multi_buf::MultiBuffer;
    use std::{thread, time};
    use rand::prelude::*;
    use std::thread::sleep;
    use crate::MemoryPool;
    use lazy_static::lazy_static;
    lazy_static!{
        pub static ref mem_pool:MemoryPool<Vec<u8>> = MemoryPool::new(4,||{vec![0u8;16]});
    }

    #[test]
    pub fn test_all_case() {
        let mut buffers = MultiBuffer::new(4,"sector_1",&mem_pool);
        assert_eq!(buffers.len(), 0);
        //write /read in first
        let test5_ele = vec![3, 5, 6, 7, 3];
        buffers.write(3, 5, &test5_ele[..]);

        assert_eq!(buffers.len(), 16);
        let mut out = vec![0u8; 5];
        buffers.read(3, 5, &mut out[..]);

        assert_eq!(test5_ele, out);

        let test30_ele = vec![17u8; 30];

        buffers.write(19, 30, &test30_ele[..]);
        assert_eq!(buffers.len(), 64);

        let mut out = vec![0u8; 30];
        buffers.read(19, 30, &mut out[..]);
        assert_eq!(test30_ele, out)
    }
    #[test]
    pub fn test_sleep() {
        rayon::scope(|s| {

            s.spawn(|_s|{
                let mut buffers = MultiBuffer::new(4,"sector_1",&mem_pool);
                // 16 * 2
                assert_eq!(buffers.len(), 0);
                //write /read in first
                let test5_ele = vec![3, 5, 6, 7, 3];
                buffers.write(16, 5, &test5_ele[..]);
                assert_eq!(buffers.len(), 32);
                sleep(time::Duration::from_secs(2));

                let test5_ele = vec![3, 5, 6, 7, 3];
                buffers.write(32, 5, &test5_ele[..]);
                assert_eq!(buffers.len(), 48);

            });

            s.spawn(|_s|{
                let mut buffers = MultiBuffer::new(4,"sector_2",&mem_pool);
                // 16 * 2
                assert_eq!(buffers.len(), 0);
                //write /read in first
                let test5_ele = vec![3, 5, 6, 7, 3];
                buffers.write(16, 5, &test5_ele[..]);
                assert_eq!(buffers.len(), 32);
                sleep(time::Duration::from_secs(3));

                let test5_ele = vec![3, 5, 6, 7, 3];
                buffers.write(32, 5, &test5_ele[..]);
                assert_eq!(buffers.len(), 48);
            });

            s.spawn(|_s|{
                let mut buffers = MultiBuffer::new(4,"sector_3",&mem_pool);
                // 16 * 2
                assert_eq!(buffers.len(), 0);
                //write /read in first
                let test5_ele = vec![3, 5, 6, 7, 3];
                buffers.write(16, 5, &test5_ele[..]);
                assert_eq!(buffers.len(), 32);
                sleep(time::Duration::from_secs(5));

                let test5_ele = vec![3, 5, 6, 7, 3];
                buffers.write(32, 5, &test5_ele[..]);
                assert_eq!(buffers.len(), 48);
            });
        });
        let mut buffers = MultiBuffer::new(4,"sector_1",&mem_pool);
        assert_eq!(buffers.len(), 0);
        //write /read in first
        let test5_ele = vec![3, 5, 6, 7, 3];
        buffers.write(3, 5, &test5_ele[..]);

        assert_eq!(buffers.len(), 16);
        let mut out = vec![0u8; 5];
        buffers.read(3, 5, &mut out[..]);

        assert_eq!(test5_ele, out);

        let test30_ele = vec![17u8; 30];

        buffers.write(19, 30, &test30_ele[..]);
        assert_eq!(buffers.len(), 64);

        let mut out = vec![0u8; 30];
        buffers.read(19, 30, &mut out[..]);
        assert_eq!(test30_ele, out)
    }
    #[test]
    pub fn test_wait() {

        rayon::scope(|s| {
            for j in 0..100 as usize {
                let i = j;
                s.spawn(move |s| {
                    let mut buffers = MultiBuffer::new(32,"sector_1",&mem_pool);

                    let mut rng = rand::thread_rng();
                    let y: f64 = rng.gen(); // generates a float between 0 and 1
                    sleep(std::time::Duration::from_millis((y * 10000.0) as u64));
                    println!("allocating:{}", i);
                    let val = vec![0u8; 1<<32];
                    buffers.write(0, 1<<32, &val[..]);
                    let mut rng = rand::thread_rng();
                    let y: f64 = rng.gen(); // generates a float between 0 and 1
                    sleep(std::time::Duration::from_millis((y * 10000.0) as u64));
                    println!("finished:{}", i);
                });
            }
        })
    }
}
