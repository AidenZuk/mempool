use crossbeam::channel;
pub struct Semphore{
    signal:(channel::Sender<()>,channel::Receiver<()>),
    name:String
}

impl Semphore {
    pub fn new(name:&str,counts:usize)->Self{

        let value = Semphore {
            signal:channel::unbounded(),
            name:String::from(name)
        };
        for i in 0 .. counts {
            value.signal.0.send(());
        }
        value
    }

    pub fn get(&self){
        log::trace!("lock sem:{}",self.name);
        self.signal.1.recv().unwrap();
    }
    pub fn put(&self){
        self.signal.0.send(()).unwrap();
        log::trace!("release sem:{}",self.name);
    }
}

#[cfg(test)]
mod Tests {
    use super::Semphore;
    #[test]
    fn test_get(){
        let single = Semphore::new("test",2);

        single.get();
        println!("get 1");

        single.get();
        println!("get 2");

        single.put();
        println!("put back");


    }
}