use std::borrow::Borrow;
use std::collections::hash_map::RandomState;
use std::hash::BuildHasher;
use std::hash::Hash;
use std::hash::Hasher;
use std::mem;
use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};

pub trait Store<K, V>: Send {
    fn set(&self, key: K, val: V);
    fn remove<T: Borrow<K>>(&self, key: T) -> bool;
    fn remove_if<T: Borrow<K>, F: Fn(&V) -> bool>(&self, key: T, cond: F) -> bool;
    fn get<T: Borrow<K>>(&self, key: T) -> Option<V>;
    fn contains<T: Borrow<K>>(&self, key: T) -> bool;
    fn for_each<F: FnMut(&K, &V) -> ()>(&self, f: F);
}

type Wrap<K, V> = Arc<RwLock<Node<K, V>>>;

pub struct ConcurrentHashtable<K, V, S = RandomState> {
    shards: Vec<Shard<K, V>>,
    hash_builder: S,
}

unsafe impl<K, V, S> Send for ConcurrentHashtable<K, V, S> {}

impl<K, V> ConcurrentHashtable<K, V, RandomState>
where
    K: Default,
    V: Default,
{
    pub fn with_shards(no_shards: usize) -> Self {
        let mut shards = Vec::with_capacity(no_shards);
        for _ in 0..no_shards {
            shards.push(Shard::default());
        }
        Self {
            shards,
            hash_builder: RandomState::new(),
        }
    }
}

impl<K, V, S> Store<K, V> for ConcurrentHashtable<K, V, S>
where
    K: Hash + PartialEq + PartialOrd,
    V: Clone,
    S: BuildHasher,
{
    fn get<T: Borrow<K>>(&self, key: T) -> Option<V> {
        let hash = self.get_hash(key.borrow());
        let shard = &self.shards[hash];
        shard.get(key)
    }

    fn contains<T: Borrow<K>>(&self, key: T) -> bool {
        self.get(key).is_some()
    }

    fn set(&self, key: K, val: V) {
        let hash = self.get_hash(key.borrow());
        let shard = &self.shards[hash];
        shard.set(key, val)
    }

    fn remove<T: Borrow<K>>(&self, key: T) -> bool {
        let hash = self.get_hash(key.borrow());
        let shard = &self.shards[hash];
        shard.remove(key)
    }

    fn remove_if<T: Borrow<K>, F: Fn(&V) -> bool>(&self, key: T, cond: F) -> bool {
        let hash = self.get_hash(key.borrow());
        let shard = &self.shards[hash];
        shard.remove_if(key, cond)
    }

    fn for_each<F: FnMut(&K, &V) -> ()>(&self, mut f: F) {
        for shard in &self.shards {
            shard.for_each(&mut f);
        }
    }
}


impl<K, V, S> ConcurrentHashtable<K, V, S>
where
    K: Hash,
    S: BuildHasher,
{
    fn get_hash(&self, key: &K) -> usize {
        let mut hasher = self.hash_builder.build_hasher();
        key.borrow().hash(&mut hasher);
        hasher.finish() as usize % self.shards.len()
    }
}

#[derive(Debug)]
struct Shard<K, V> {
    head: Wrap<K, V>,
}

impl<K, V> Default for Shard<K, V>
where
    K: Default,
    V: Default,
{
    fn default() -> Self {
        Self {
            head: Arc::new(RwLock::new(Node::default())),
        }
    }
}

impl<K, V> Shard<K, V>
where
    K: PartialEq + PartialOrd,
    V: Clone,
{
    pub fn get<T: Borrow<K>>(&self, key: T) -> Option<V> {
        let lock = self.head.read().unwrap();
        if let None = &lock.next {
            return None;
        }
        let next = Arc::clone(&lock.next.as_ref().unwrap());
        Self::get_util(key, next, lock)
    }

    pub fn for_each<F: FnMut(&K, &V) -> ()>(&self, mut f: F) {
        let lock = self.head.read().unwrap();
        if let None = &lock.next {
            return;
        }
        let next = Arc::clone(&lock.next.as_ref().unwrap());
        Self::iter_util(&mut f, next, lock)
    }

    fn iter_util <F: FnMut(&K, &V) -> ()>(
        mut f: F,
        node: Wrap<K, V>,
        prev_lock: RwLockReadGuard<'_, Node<K, V>>,
    ) {
        let lock = node.read().unwrap();
        mem::drop(prev_lock);
        f(&lock.key, &lock.val);
        match &lock.next {
            None => {},
            Some(node) => {
                let next = Arc::clone(node);
                Self::iter_util(f, next, lock);
            }
        }
    }

    fn get_util<T: Borrow<K>>(
        key: T,
        node: Wrap<K, V>,
        prev_lock: RwLockReadGuard<'_, Node<K, V>>,
    ) -> Option<V> {
        let lock = node.read().unwrap();
        mem::drop(prev_lock);
        if &lock.key == key.borrow() {
            return Some(lock.val.clone());
        }
        if &lock.key > key.borrow() {
            return None;
        }
        match &lock.next {
            None => None,
            Some(node) => {
                let next = Arc::clone(node);
                Self::get_util(key, next, lock)
            }
        }
    }

    pub fn remove<T: Borrow<K>>(&self, key: T) -> bool{
        let always_true: fn(&V) -> bool = |_| true;
        let lock = self.head.write().unwrap();
        if let None = &lock.next {
            false
        } else {
            let node = Arc::clone(&lock.next.as_ref().unwrap());
            let lock_next = node.write().unwrap();
            Self::remove_util(key, always_true, lock_next, lock)
        }
    }
 
    fn remove_if<T: Borrow<K>, F: Fn(&V) -> bool>(&self, key: T, cond: F) -> bool {
        let lock = self.head.write().unwrap();
        if let None = &lock.next {
            return false;
        } else {
            let node = Arc::clone(&lock.next.as_ref().unwrap());
            let lock_next = node.write().unwrap();
            Self::remove_util(key, cond, lock_next, lock)
        }
    }

    fn remove_util<T: Borrow<K>, F: Fn(&V) -> bool>(
        key: T,
        cond: F,
        mut lock: RwLockWriteGuard<'_, Node<K, V>>,
        mut prev_lock: RwLockWriteGuard<'_, Node<K, V>>,
    ) -> bool {
        if &lock.key > key.borrow() {
            return false;
        }
        if &lock.key == key.borrow() {
            if !cond(&lock.val) {
                return false;
            }
            let next = mem::replace(&mut lock.next, None);
            prev_lock.next = next;
            return true;
        }

        match &lock.next {
            None => false,
            Some(next) => {
                let next = Arc::clone(next);
                let next_lock = next.as_ref().write().unwrap();
                mem::drop(prev_lock);
                Self::remove_util(key, cond, next_lock, lock)
            }
        }
    }

    pub fn set(&self, key: K, val: V) {
        let mut lock = self.head.write().unwrap();
        if let None = &lock.next {
            lock.next = Some(Arc::new(RwLock::new(Node {
                key,
                val,
                next: None,
            })));
            return;
        } else {
            let node = Arc::clone(&lock.next.as_ref().unwrap());
            let lock_next = node.write().unwrap();
            Self::set_util(key, val, lock_next, lock);
        }
    }

    fn set_util(
        key: K,
        val: V,
        mut lock: RwLockWriteGuard<'_, Node<K, V>>,
        mut prev_lock: RwLockWriteGuard<'_, Node<K, V>>,
    ) {
        if &lock.key > &key {
            let next = mem::replace(&mut prev_lock.next, None);
            prev_lock.next = Some(Arc::new(RwLock::new(Node { key, val, next })));
            return;
        }

        if &lock.key == &key {
            lock.val = val;
            return;
        }

        match &lock.next {
            None => {
                lock.next = Some(Arc::new(RwLock::new(Node {
                    key,
                    val,
                    next: None,
                })));
            }
            Some(next) => {
                let next = Arc::clone(next);
                let next_lock = next.as_ref().write().unwrap();
                mem::drop(prev_lock);
                Self::set_util(key, val, next_lock, lock);
            }
        };
    }
}



#[derive(Default, Debug)]
struct Node<K, V> {
    key: K,
    val: V,
    next: Option<Wrap<K, V>>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread::scope;

    fn own(s: &str) -> String {
        s.to_string()
    }

    #[test]
    fn set_and_get() {
        let shard: Shard<String, String> = Shard::default();
        shard.set(own("a"), own("1"));
        shard.set(own("c"), own("2"));
        shard.set(own("b"), own("3"));
        assert_eq!(shard.get(own("a")), Some(own("1")));
        assert_eq!(shard.get(own("c")), Some(own("2")));
        assert_eq!(shard.get(own("b")), Some(own("3")));
    }

    #[test]
    fn set_and_remove() {
        let shard: Shard<String, String> = Shard::default();
        shard.set(own("a"), own("1"));
        shard.set(own("c"), own("2"));
        shard.set(own("b"), own("3"));
        shard.remove(own("c"));
        println!("{shard:?}");
        assert_eq!(shard.get(own("a")), Some(own("1")));
        assert_eq!(shard.get(own("b")), Some(own("3")));
        assert_eq!(shard.get(own("c")), None);
    }

    #[test]
    fn set_multithreaded() {
        let shard: Shard<String, String> = Shard::default();
        scope(|scope| {
            let _h1 = scope.spawn(|| {
                for i in 0..500 {
                    shard.set(i.to_string(), i.to_string());
                }
            });
            let _h2 = scope.spawn(|| {
                for i in 500..1000 {
                    shard.set(i.to_string(), i.to_string());
                }
            });
            let _h3 = scope.spawn(|| {
                for i in 1000..1500 {
                    shard.set(i.to_string(), i.to_string());
                }
            });
        });
        for i in 0..1500 {
            assert_eq!(shard.get(i.to_string()), Some(i.to_string()));
        }
    }

    #[test]
    fn remove_multithreaded() {
        let shard: Shard<String, String> = Shard::default();
        for i in 0..1000 {
            shard.set(i.to_string(), i.to_string());
        }

        scope(|scope| {
            let _h1 = scope.spawn(|| {
                for i in (0..1000).step_by(2) {
                    shard.remove(i.to_string());
                }
            });
            let _h2 = scope.spawn(|| {
                for i in (1..1000).step_by(2) {
                    shard.remove(i.to_string());
                }
            });
        });
        for i in 0..1000 {
            assert_eq!(shard.get(i.to_string()), None);
        }
    }
}
