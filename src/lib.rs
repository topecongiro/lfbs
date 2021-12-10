//! Lock-free stack with batched pop.
//!
//! ## No memory relamation scheme
//! 
//! Usually, lock-free data structures must be used with a memory reclamation scheme like 
//! epoch-based reclamation or hazard pointer to guarantee the safe removal from the data structure.
//! This lock-free stack overcomes this limitation by not accessing any node in the data structure 
//! while performing the removal operation. The caveat is that it is no longer possible to remove 
//! a single element at a time; instead, every element in the stack will be removed at once.


use std::{
    ptr::{self, NonNull},
    sync::atomic::{AtomicPtr, Ordering},
};

/// Lock-free stack with batched pop.
pub struct Stack<T> {
    ptr: AtomicPtr<Node<T>>,
}

unsafe impl<T: Send> Sync for Stack<T> {}
unsafe impl<T: Send> Send for Stack<T> {}

struct Node<T> {
    next: Option<NonNull<Node<T>>>,
    val: T,
}

pub struct Iter<T> {
    next: Option<NonNull<Node<T>>>,
}

impl<T> Stack<T> {
    /// Creates a new empty `Stack`.
    pub fn new() -> Self {
        Stack {
            ptr: AtomicPtr::new(ptr::null_mut()),
        }
    }

    /// Inserts an element to the stack.
    pub fn push(&self, val: T) {
        let mut current = self.ptr.load(Ordering::Relaxed);
        let new_node = Box::into_raw(Box::new(Node { next: None, val }));
        loop {
            unsafe {
                (*new_node).next = NonNull::new(current);
            }
            let result = self.ptr.compare_exchange_weak(
                current,
                new_node,
                Ordering::Relaxed,
                Ordering::Relaxed,
            );

            match result {
                Ok(..) => return,
                Err(new_current) => {
                    current = new_current;
                }
            }
        }
    }

    /// Removes every element from the stack and returns them as an iterator, or `None` if it is empty.
    pub fn pop(&self) -> Option<Iter<T>> {
        let mut current = self.ptr.load(Ordering::Relaxed);

        loop {
            let result = self.ptr.compare_exchange_weak(
                current,
                ptr::null_mut(),
                Ordering::Relaxed,
                Ordering::Relaxed,
            );

            match result {
                Ok(ptr) => {
                    return Some(Iter {
                        next: NonNull::new(ptr),
                    })
                }
                Err(ptr) if ptr.is_null() => return None,
                Err(ptr) => current = ptr,
            }
        }
    }
}

impl<T> Iterator for Iter<T> {
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        match self.next {
            Some(node) => unsafe {
                let n = Box::from_raw(node.as_ptr());
                self.next = n.next;
                Some(n.val)
            },
            None => None,
        }
    }
}

impl<T> Drop for Iter<T> {
    fn drop(&mut self) {
        while let Some(_) = self.next() {}
    }
}

impl<T> Drop for Stack<T> {
    fn drop(&mut self) {
        self.pop();
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::sync::{atomic::AtomicUsize, Arc};

    struct DropCount<'a, T> {
        val: T,
        count: &'a AtomicUsize,
    }

    impl<'a, T> Drop for DropCount<'a, T> {
        fn drop(&mut self) {
            self.count.fetch_add(1, Ordering::Relaxed);
        }
    }

    #[test]
    fn multithread_push() {
        use std::collections::HashMap;

        let work_size = 100000;
        let thread_num = num_cpus::get();
        let s = Arc::new(Stack::new());
        let mut ts = vec![];

        for i in 0..thread_num / 2 {
            let s = Arc::clone(&s);
            ts.push(std::thread::spawn(move || {
                for j in i * work_size..(i + 1) * work_size {
                    s.push(j);
                }
            }));
        }

        let seen = Arc::new(AtomicUsize::new(0));
        let mut consumers = vec![];
        {
            for _ in 0..thread_num / 2 {
                let s = Arc::clone(&s);
                let seen = Arc::clone(&seen);
                consumers.push(std::thread::spawn(move || {
                    let mut map = HashMap::new();
                    while seen.load(Ordering::Relaxed) < work_size * (thread_num / 2) {
                        if let Some(iter) = s.pop() {
                            for v in iter {
                                let e = map.entry(v).or_insert(0);
                                *e += 1;
                                (&seen).fetch_add(1, Ordering::Relaxed);
                            }
                        }
                    }
                    map
                }));
            }
        }

        for t in ts {
            t.join().unwrap();
        }
        let mut maps = vec![];
        for t in consumers {
            maps.push(t.join().unwrap());
        }

        for i in 0..seen.load(Ordering::Relaxed) {
            let mut found = false;
            for m in &maps {
                if let Some(&count) = m.get(&i) {
                    assert_eq!(count, 1);
                    found = true;
                    break;
                }
            }
            assert!(found);
        }
    }

    #[test]
    fn drop_count() {
        let count = 1000000usize;
        let drop = AtomicUsize::new(0);
        {
            let s = Stack::new();
            for i in 0..count {
                s.push(DropCount {
                    val: i,
                    count: &drop,
                });
            }
        }
        assert_eq!(drop.load(Ordering::Relaxed), count);
    }
}
