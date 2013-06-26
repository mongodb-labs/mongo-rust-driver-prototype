use std::hash::Hash;
use std::hashmap::*;
use std::container::Container;
use std::option::Option;
use std::iterator::IteratorUtil;

///A hashmap which maintains iteration order using a list.
pub struct OrderedHashmap<K,V> {
    priv map: HashMap<K,V>,
    priv order: ~[(@K,@V)]
}

impl<K: Hash + Eq,V> Container for OrderedHashmap<K,V> {
    pub fn len(&const self) -> uint { self.map.len() }
    pub fn is_empty(&const self) -> bool { self.map.is_empty() }
}

impl<K: Hash + Eq,V> Mutable for OrderedHashmap<K,V> {
    pub fn clear(&mut self) {
        self.map = HashMap::new();
        self.order = ~[];
    }
}

impl<K:Hash + Eq,V: Eq> Eq for OrderedHashmap<K,V> {
    fn eq(&self, other: &OrderedHashmap<K,V>) -> bool {
        self.map == other.map && self.order == other.order
    }
    fn ne(&self, other: &OrderedHashmap<K,V>) -> bool {
        self.map != other.map || self.order != other.order
    }
}
/**Expose most of the Hashmap implementation.
* TODO: Still exposes old iterator syntax.
*/
impl<K: Hash + Eq + Copy,V: Copy> OrderedHashmap<K,V> {
    pub fn len(&self) -> uint { self.map.len() }
    pub fn contains_key(&self, k: &K) -> bool { self.map.contains_key(k) }
    pub fn each(&self, blk: &fn(&K, & V) -> bool) -> bool {
        for self.order.iter().advance |&(k, v)| {
            if !blk(k, v) { return false; }
        }
        true
    }
    pub fn each_key(&self, blk: &fn(&K) -> bool) -> bool {
        for self.order.iter().advance |&(k, _)| {
            if !blk(k) { return false; }
        }
        true
    }
    pub fn each_value(& self, blk: &fn(&V) -> bool) -> bool {
        for self.order.iter().advance |&(_, v)| {
            if !blk(v) { return false; }
        }
        true
    }
    pub fn find<'a>(&'a self, k: &K) -> Option<&'a V> {
        self.map.find(k)
    }
    pub fn find_mut<'a>(&'a mut self, k: &K) -> Option<&'a mut V> {
        self.map.find_mut(k)
    }
    pub fn insert(&mut self, k: K, v: V) -> bool {
        let success = self.map.insert(copy k, copy v);
        if success { self.order.push((@k, @v)) }
        success
    }

    pub fn new() -> OrderedHashmap<K,V> {
        OrderedHashmap { map: HashMap::new(), order: ~[] }
    }
}
