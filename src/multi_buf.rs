use std::{vec, fs};
use anyhow::{*, Result};
use log::{info, trace};
use std::env;
use std::thread;
use crate::{Reusable, MemoryPool};
use crossbeam::crossbeam_channel;

const ENV_MEMORY_CACHE: &str = "FIL_PROOFS_MEMORY_CACHE_PATH";
macro_rules! prefetch {
    ($val:expr) => {
        #[cfg(any(target_arch = "x86_64", target_arch = "x86"))]
        unsafe {
            #[cfg(target_arch = "x86")]
            use std::arch::x86::*;
            #[cfg(target_arch = "x86_64")]
            use std::arch::x86_64::*;

            _mm_prefetch($val, _MM_HINT_T2);
        }
    };
}
macro_rules! prefetchl0 {
    ($val:expr) => {
        #[cfg(any(target_arch = "x86_64", target_arch = "x86"))]
        unsafe {
            #[cfg(target_arch = "x86")]
            use std::arch::x86::*;
            #[cfg(target_arch = "x86_64")]
            use std::arch::x86_64::*;

            _mm_prefetch($val, _MM_HINT_T0);
        }
    };
}
pub struct MultiBuffer {
    seg_exp: u8,
    seg_len: usize,
    seg_len_mask: usize,
    //fast calculation
    buffers: Vec<Reusable<Vec<u8>>>,
    total_len: usize,
    mem_pool: &'static MemoryPool<Vec<u8>>,
    id: String,
    store_files: Vec<PathBuf>,
    path_parent: PathBuf,
}

// use storage_proofs_core::{
//     util::{data_at_node_offset, NODE_SIZE},
// };
use std::thread::sleep;
use std::sync::mpsc::channel;
use std::ops::{Deref, DerefMut};
use std::fs::OpenOptions;
use std::io::{Read, Write};
use std::path::PathBuf;
use std::process::Command;

const NODE_SIZE: usize = 32;

pub struct SEG_PT {
    pub segment: u8,
    pub offset: u32,
}

impl Drop for MultiBuffer {
    fn drop(&mut self) {
        Command::new("mkdir")
            .arg("-p")
            .arg(&self.path_parent.parent().unwrap().to_path_buf())
            .output()
            .expect("failed to create cache path");
    }
}

impl MultiBuffer {
    pub fn new(seg_exp: u8, id: &str, mem_pool: &'static MemoryPool<Vec<u8>>) -> Self {
        let seg_len = (1 << seg_exp);
        let mut path_parent = if let Ok(path_cache) = env::var(ENV_MEMORY_CACHE) {
            PathBuf::from(path_cache)
        } else if let Ok(path_cache) = env::var("TMPDIR") {
            PathBuf::from(path_cache)
        } else {
            PathBuf::from("/var/tmp")
        };
        path_parent = path_parent.join("mem_cache").join(id);
        Command::new("mkdir")
            .arg("-p")
            .arg(&path_parent)
            .output()
            .expect("failed to create cache path");
        MultiBuffer {
            seg_exp,
            seg_len,
            seg_len_mask: seg_len - 1,
            buffers: Vec::new(),
            total_len: 0,
            mem_pool,
            id: String::from(id),
            path_parent,
            store_files: Vec::new(),
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
    pub fn buffers_mut(&mut self) -> Vec<&mut Vec<u8>> {
        let mut buf_items = Vec::with_capacity(self.buffers.len());
        self.buffers.iter_mut().for_each(|item| {
            buf_items.push(item.deref_mut());
        });
        buf_items
    }

    pub fn buffers(&self) -> Vec<&Vec<u8>> {
        let mut buf_items = Vec::with_capacity(self.buffers.len());
        self.buffers.iter().for_each(|item| {
            buf_items.push(item.deref());
        });
        buf_items
    }
    pub fn get_seg_exp(&self) -> u8 {
        self.seg_exp
    }
    pub fn get_seg_len(&self) -> usize {
        self.seg_len
    }
    pub fn get_seg_mask(&self) -> usize {
        self.seg_len_mask
    }
    #[inline]
    pub fn read_index_no_extend(&self, i: usize, parents: &[(u16, u32)]) -> &[u8] {
        &self.buffers[parents[i].0 as usize][parents[i].1 as usize..parents[i].1 as usize + NODE_SIZE]
    }
    // pub fn read_indice(&mut self,parents: &[u32])->&[&'a [u8]]{
    //
    // }
    #[inline]
    pub fn read_index(&mut self, i: usize, parents: &[u32]) -> &[u8] {
        let start = parents[i] as usize * NODE_SIZE;
        let len = NODE_SIZE;
        if (start >= self.total_len) || (start + len > self.total_len) {
            //prepare buffer
            //if start + len >= self.total_len {
            //计算需要几个buffer
            let buff_counts = ((start + len + self.seg_len - 1) >> self.seg_exp);
            for _i in self.buffers.len()..buff_counts {
                self.get_memory();
            }
            self.total_len = self.buffers.len() << self.seg_exp;
            // }
        }
        let mut start_seg = start >> self.seg_exp;
        let end_seg = (start + len - 1) >> self.seg_exp;
        let seg_offset = start & (self.seg_len_mask);
        unsafe {
            if start_seg == end_seg {
                //std::ptr::copy_nonoverlapping((&self.buffers[start_seg][seg_offset..seg_offset + len]).as_ptr(), target[..].as_mut_ptr(), len);
//                    (&mut target[..len]).clone_from_slice(&self.buffers[start_seg][seg_offset..seg_offset+len]);
                &self.buffers[start_seg][seg_offset..seg_offset + len]
            } else {
                panic!(anyhow!("this should be in same seg"));
            }
        }
    }
    #[inline]
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

    fn get_memory(&mut self) {
        let (sender, receiver) = crossbeam_channel::unbounded();
        let min_req = self.buffers.len();
        let (item, should_sleep) = self.mem_pool.pending(&self.id, sender.clone(), min_req);

        if let Some(item) = item {
            log::trace!("{} get an item,left:{}", self.id.clone(), self.mem_pool.len());
            self.buffers.push(item);
        } else if !should_sleep {
            log::trace!("{} pending an item ", self.id.clone());
            let result = receiver.recv().unwrap();
            log::trace!("{} pending finished ", self.id.clone());

            self.buffers.push(result);
        } else {

            log::trace!("{} sleep with min_req:{} ", self.id.clone(), min_req);

            self.sleep();

            log::trace!("{} sleeped ok:{} ", self.id.clone(), min_req);
            let mut recovered = 0;
            loop {
                let result = receiver.recv().unwrap();
                self.buffers.push(result);
                recovered += 1;
                if recovered > min_req {
                    break;
                }
            }

            log::trace!("{} waked up with min_req:{} ", self.id.clone(), min_req);

            //恢复
            self.wakeup();
            log::trace!("{} waked up !!!! ", self.id.clone());
        }
    }
    #[inline]
    pub fn write_ensured(&mut self, start: usize, len: usize, source: &[u8]) {
        unsafe {
            std::ptr::copy_nonoverlapping((&source).as_ptr(), self.buffers[start >> self.seg_exp][start & (self.seg_len_mask)..].as_mut_ptr(), len);
            //  &self.buffers[start_seg][seg_offset..seg_offset+len].clone_from_slice(&source[..len]);
        }
    }
    #[inline]
    pub fn read_ensured(&self, start: usize, len: usize, source: &mut [u8]) {
        unsafe {
            std::ptr::copy_nonoverlapping(self.buffers[start >> self.seg_exp][start & (self.seg_len_mask)..].as_ptr(), source.as_mut_ptr(), len);
            //  &self.buffers[start_seg][seg_offset..seg_offset+len].clone_from_slice(&source[..len]);
        }
    }
    #[inline]
    pub fn write(&mut self, start: usize, len: usize, source: &[u8]) -> Result<()> {
        // if len == 1 {
        //     log::trace!("???")
        // }
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

    pub fn ensure(&mut self, len: usize) {
        if len > self.total_len {
            //计算需要几个buffer
            let buff_counts = ((len + self.seg_len - 1) >> self.seg_exp);
            for _i in self.buffers.len()..buff_counts {
                self.get_memory();
            }

            self.total_len = self.buffers.len() << self.seg_exp;
            //  log::trace!("total len is : {},and seg is:{}, with request:{}, seg_exp :{}",self.total_len,self.buffers.len(),len,self.seg_exp);
        }
    }
    fn sleep(&mut self) {
        log::trace!("{} sleeping with min_req:{}", self.id.clone(), &self.buffers.len() + 1);
        self.store_files.clear();
        for (i, val) in self.buffers.iter().enumerate() {
            let file_name = self.path_parent.join(format!("cache_{}.dat", i));
            fs::write(&file_name, val.deref());
            self.store_files.push((&file_name).clone());
            drop(val)
        }
        drop(&self.buffers);
        self.buffers = Vec::new();
        log::trace!("{} slept ok",self.id.clone())
        // sleep(std::time::Duration::from_secs(3));
    }
    fn wakeup(&mut self) {
        let total_count = self.store_files.len();
        log::trace!("{} wake up with min_req:{}", self.id.clone(), total_count+1);
        for file_id in 0..total_count {
            let file_path = self.path_parent.join(format!("cache_{}.dat", file_id));
            let mut open_option = OpenOptions::new();

            let data = self.buffers[file_id].deref_mut();
            if let Ok(mut file) = open_option.read(true).open(&file_path) {
                let file_len = file.metadata().unwrap().len();
                file.read_exact(&mut data[..file_len as usize]);
                //file.read_to_end(data);

                log::trace!("remove mem cache file:{:?}", &file_path.display());
                fs::remove_file(&file_path);
            } else {
                panic!("memory cache lost:{:?}", file_path.display())
            }
        }
        log::trace!("wake up finished!");
        self.store_files.clear();
    }

    #[inline]
    pub fn prefetch(&self, parents: &[(u16, u32)]) {
        for parent in parents {
            prefetchl0!(self.buffers[parent.0 as usize ][parent.1 as usize..parent.1 as usize + NODE_SIZE].as_ptr() as *const i8);
        }
    }

    pub fn load_from_file(&mut self, file_name: &PathBuf) -> usize {
        let mut open_option = fs::OpenOptions::new();
        let mut file = open_option.read(true).open(&file_name).unwrap();
        let mut read_size = 0;
        let total_size = file.metadata().unwrap().len() as usize;
        if total_size >= self.total_len {
            //计算需要几个buffer
            let buff_counts = ((total_size + self.seg_len - 1) >> self.seg_exp);
            for _i in self.buffers.len()..buff_counts {
                self.get_memory();
            }
            self.total_len = self.buffers.len() << self.seg_exp;
        }

        let mut all_read_size = 0;
        for i in 0..self.buffers.len() {
            let mut cur_round_size = 0;
            loop {
                let mut read_size = file.read(&mut self.buffers[i][cur_round_size..]).unwrap();
                cur_round_size += read_size;
                all_read_size += read_size;
                if cur_round_size >= self.seg_len || all_read_size >= total_size {
                    break;
                } else if read_size == 0 {
                    panic!("read file failed :{} of {} read", all_read_size, total_size);
                }
            }
        }
        all_read_size
    }
    pub fn write_to_file(&self, file_name: &PathBuf) -> usize {
        let mut open_option = fs::OpenOptions::new();
        let mut file = open_option.create(true).write(true).open(&file_name).unwrap();
        let mut write_size_remain = self.total_len;


        for i in 0..self.buffers.len() {
            let mut current_write_size = self.seg_len;
            if current_write_size > write_size_remain {
                current_write_size = write_size_remain;
            }
            let mut write_round_size = 0;
            loop {
                match file.write(&self.buffers[i][write_round_size..current_write_size]) {
                    Ok(write_size) => {
                        write_round_size += write_size;
                        if write_round_size >= current_write_size {
                            break;
                        }
                    },
                    Err(e) => {
                        panic!("write file {:?} failed error:{} ", &file_name, e.to_string());
                    }
                }
            }
        }
        self.total_len
    }
}

pub trait GetSegs<T> {
    fn get_buffers(&self) -> Vec<(&Vec<u8>, usize)>;
}

impl GetSegs<u8> for MultiBuffer {
    fn get_buffers(&self) -> Vec<(&Vec<u8>, usize)> {
        let mut result = Vec::new();
        let total_segs = self.buffers.len() - 1;
        let last_len = self.total_len - self.seg_len * (total_segs);
        for (i, val) in self.buffers.iter().enumerate() {
            if i != total_segs {
                result.push((val.deref(), self.seg_len))
            } else {
                result.push((val.deref(), last_len))
            }
        }
        result
    }
}

#[cfg(test)]
mod Tests {
    use crate::multi_buf::MultiBuffer;
    use std::{thread, time};
    use rand::prelude::*;
    use std::thread::sleep;
    use crate::MemoryPool;
    use lazy_static::lazy_static;
    use rand;

    const buf_len: usize = 1 << 12;
    lazy_static! {
        pub static ref mem_pool:MemoryPool<Vec<u8>> = MemoryPool::new(7,||{vec![0u8;buf_len]});
    }

    #[test]
    pub fn test_all_case() {
        let mut buffers = MultiBuffer::new(12, "sector_1", &mem_pool);
        assert_eq!(buffers.len(), 0);
        //write /read in first
        let test5_ele = vec![3, 5, 6, 7, 3];
        buffers.write(buf_len - 5, 5, &test5_ele[..]);

        assert_eq!(buffers.len(), buf_len);
        let mut out = vec![0u8; 5];
        buffers.read(buf_len - 5, 5, &mut out[..]);

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
            for i in 0..4
            {
                s.spawn(move |_s| {
                    let mut buffers = MultiBuffer::new(12, &format!("sector_{}", i), &mem_pool);
                    // 16 * 2
                    assert_eq!(buffers.len(), 0);
                    //write /read in first
                    let test5_ele = vec![3, 5, 6, 7, 3];
                    buffers.write(buf_len - 5, 5, &test5_ele[..]);
                    assert_eq!(buffers.len(), buf_len);
                    //write /read in first
                    let test5_ele = vec![3, 5, 6, 7, 3];
                    buffers.write(buf_len, 5, &test5_ele[..]);
                    assert_eq!(buffers.len(), buf_len * 2);
                    sleep(time::Duration::from_millis(rand::random::<u8>() as u64 * 10));
                    log::trace!("------------sector_{} asked for more buffer",i);
                    let test5_ele = vec![3, 5, 6, 7, 3];
                    buffers.write(buf_len * 2, 5, &test5_ele[..]);
                    assert_eq!(buffers.len(), buf_len * 3);
                    //println!("------------sector_{} finsihed",i);
                });
            }
        });
    }

    #[test]
    pub fn test_wait() {
        rayon::scope(|s| {
            for j in 0..100 as usize {
                let i = j;
                s.spawn(move |s| {
                    let mut buffers = MultiBuffer::new(32, "sector_1", &mem_pool);

                    let mut rng = rand::thread_rng();
                    let y: f64 = rng.gen(); // generates a float between 0 and 1
                    sleep(std::time::Duration::from_millis((y * 10000.0) as u64));
                    //println!("allocating:{}", i);
                    let val = vec![0u8; 1 << 32];
                    buffers.write(0, 1 << 32, &val[..]);
                    let mut rng = rand::thread_rng();
                    let y: f64 = rng.gen(); // generates a float between 0 and 1
                    sleep(std::time::Duration::from_millis((y * 10000.0) as u64));
                    //println!("finished:{}", i);
                });
            }
        })
    }
}
