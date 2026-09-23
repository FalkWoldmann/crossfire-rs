use crate::*;
use crossfire::flavor::{self, Queue};
use crossfire::oneshot::{RxOneshot, TxOneshot};
use crossfire::select::Multiplex;
use crossfire::waitgroup::{WaitGroup, WaitGroupGuard, WaitGroupZero, WaitGroupZeroGuard};
use crossfire::*;
use std::cell::Cell;
use std::future::{pending, Future, Ready};
use std::marker::PhantomData;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::pin::pin;
use std::rc::Rc;
use std::sync::atomic::AtomicU32;
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};
use std::thread;
use std::time::Duration;

// Detect auto traits of concrete types: the inherent const only exists when the bound holds,
// otherwise the trait's default const is used.
struct Check<T: ?Sized>(PhantomData<T>);

trait NotSend {
    const SEND: bool = false;
}
impl<T: ?Sized> NotSend for Check<T> {}
impl<T: ?Sized + Send> Check<T> {
    const SEND: bool = true;
}

trait NotSync {
    const SYNC: bool = false;
}
impl<T: ?Sized> NotSync for Check<T> {}
impl<T: ?Sized + Sync> Check<T> {
    const SYNC: bool = true;
}

macro_rules! is_send {
    ($t: ty) => {
        <Check<$t>>::SEND
    };
}

macro_rules! is_sync {
    ($t: ty) => {
        <Check<$t>>::SYNC
    };
}

struct NoopWaker;

impl Wake for NoopWaker {
    fn wake(self: Arc<Self>) {}
}

fn noop_waker() -> Waker {
    Waker::from(Arc::new(NoopWaker))
}

#[test]
fn test_oneshot_requires_send_item() {
    assert!(!is_send!(TxOneshot<Rc<u8>>));
    assert!(!is_sync!(TxOneshot<Rc<u8>>));
    assert!(!is_send!(RxOneshot<Rc<u8>>));
    assert!(is_send!(TxOneshot<u8>));
    assert!(is_sync!(TxOneshot<u8>));
    assert!(is_send!(RxOneshot<u8>));
}

#[test]
fn test_queue_requires_send_item() {
    assert!(!is_send!(flavor::One<Rc<u8>>));
    assert!(!is_sync!(flavor::One<Rc<u8>>));
    assert!(!is_send!(flavor::Array<Rc<u8>>));
    assert!(!is_sync!(flavor::Array<Rc<u8>>));
    assert!(!is_send!(flavor::List<Rc<u8>>));
    assert!(!is_sync!(flavor::List<Rc<u8>>));
    assert!(!is_send!(flavor::ArraySpsc<Rc<u8>>));
    assert!(!is_send!(flavor::ArrayMpsc<Rc<u8>>));
    assert!(!is_send!(flavor::OneSpsc<Rc<u8>>));
    assert!(!is_send!(flavor::OneMpsc<Rc<u8>>));

    assert!(is_send!(flavor::One<u8>));
    assert!(is_sync!(flavor::One<u8>));
    assert!(is_send!(flavor::Array<u8>));
    assert!(is_sync!(flavor::Array<u8>));
    assert!(is_send!(flavor::List<u8>));
    assert!(is_sync!(flavor::List<u8>));
}

#[test]
fn test_single_side_queue_not_sync() {
    // Queue::push()/pop() take &self, sharing these across threads would allow concurrent
    // producers / consumers they are not designed for.
    assert!(!is_sync!(flavor::ArraySpsc<u8>));
    assert!(!is_sync!(flavor::ArrayMpsc<u8>));
    assert!(!is_sync!(flavor::OneSpsc<u8>));
    assert!(!is_sync!(flavor::OneMpsc<u8>));
    assert!(!is_sync!(spsc::Array<u8>));
    assert!(!is_sync!(mpsc::Array<u8>));
    assert!(is_send!(flavor::ArraySpsc<u8>));
    assert!(is_send!(flavor::ArrayMpsc<u8>));
    assert!(is_send!(flavor::OneSpsc<u8>));
    assert!(is_send!(flavor::OneMpsc<u8>));

    // Single thread usage is still allowed
    let q = flavor::ArraySpsc::<usize>::new(2);
    q.push(1).expect("push");
    q.push(2).expect("push");
    assert_eq!(q.push(3), Err(3));
    assert_eq!(q.pop(), Some(1));
    assert_eq!(q.pop(), Some(2));
    assert_eq!(q.pop(), None);
}

#[test]
fn test_timeout_future_requires_send_timer() {
    type NotSendTimer = Ready<Rc<()>>;
    type SendTimer = Ready<()>;
    assert!(!is_send!(SendTimeoutFuture<'static, mpsc::Array<u8>, NotSendTimer, Rc<()>>));
    assert!(!is_send!(RecvTimeoutFuture<'static, mpsc::Array<u8>, NotSendTimer, Rc<()>>));
    assert!(is_send!(SendTimeoutFuture<'static, mpsc::Array<u8>, SendTimer, ()>));
    assert!(is_send!(RecvTimeoutFuture<'static, mpsc::Array<u8>, SendTimer, ()>));
}

#[test]
fn test_waitgroup_requires_sync_inner() {
    assert!(!is_send!(WaitGroup<Cell<u32>>));
    assert!(!is_send!(WaitGroupGuard<Cell<u32>>));
    assert!(!is_sync!(WaitGroupGuard<Cell<u32>>));
    assert!(!is_send!(WaitGroupZero<Cell<u32>>));
    assert!(!is_send!(WaitGroupZeroGuard<Cell<u32>>));
    assert!(!is_sync!(WaitGroupZeroGuard<Cell<u32>>));

    assert!(is_send!(WaitGroup<AtomicU32>));
    assert!(is_send!(WaitGroupGuard<AtomicU32>));
    assert!(is_sync!(WaitGroupGuard<AtomicU32>));
    assert!(is_send!(WaitGroupZero<AtomicU32>));
    assert!(is_send!(WaitGroupZeroGuard<AtomicU32>));
    assert!(is_sync!(WaitGroupZeroGuard<AtomicU32>));
}

#[test]
fn test_send_future_poll_after_ready() {
    let (tx, rx) = mpsc::bounded_async::<String>(4);
    let waker = noop_waker();
    let mut ctx = Context::from_waker(&waker);
    {
        let mut fut = pin!(tx.send("hello".to_string()));
        assert!(matches!(fut.as_mut().poll(&mut ctx), Poll::Ready(Ok(()))));
        let r = catch_unwind(AssertUnwindSafe(|| fut.as_mut().poll(&mut ctx)));
        assert!(r.is_err(), "poll after Ready should panic");
    }
    {
        let mut fut = pin!(tx.send_with_timer("world".to_string(), pending::<()>()));
        assert!(matches!(fut.as_mut().poll(&mut ctx), Poll::Ready(Ok(()))));
        let r = catch_unwind(AssertUnwindSafe(|| fut.as_mut().poll(&mut ctx)));
        assert!(r.is_err(), "poll after Ready should panic");
    }
    // Each message is delivered exactly once
    assert_eq!(rx.len(), 2);
    assert_eq!(rx.try_recv().expect("recv"), "hello");
    assert_eq!(rx.try_recv().expect("recv"), "world");
    assert!(rx.try_recv().is_err());
}

#[test]
fn test_send_future_poll_after_disconnect() {
    let (tx, rx) = mpsc::bounded_async::<String>(4);
    drop(rx);
    let waker = noop_waker();
    let mut ctx = Context::from_waker(&waker);
    let mut fut = pin!(tx.send("hello".to_string()));
    match fut.as_mut().poll(&mut ctx) {
        Poll::Ready(Err(SendError(s))) => assert_eq!(s, "hello"),
        _ => unreachable!(),
    }
    let r = catch_unwind(AssertUnwindSafe(|| fut.as_mut().poll(&mut ctx)));
    assert!(r.is_err(), "poll after Ready should panic");
}

#[test]
fn test_waitgroup_into_inner_after_wait() {
    // try_into_inner() / get_mut() must not race with a guard still waking the waiter.
    for _ in 0..ROUND {
        let mut wg = WaitGroup::new(Box::new(1usize), 0);
        let guard = wg.add_guard();
        let th = thread::spawn(move || drop(guard));
        wg.wait();
        **WaitGroup::get_mut(&mut wg).expect("get_mut") += 1;
        assert_eq!(*WaitGroup::try_into_inner(wg).expect("into_inner"), 2);
        th.join().expect("join");

        let wg = WaitGroupZero::new(Box::new(1usize));
        let guard = wg.add_guard();
        let th = thread::spawn(move || drop(guard));
        wg.wait();
        assert_eq!(*WaitGroupZero::try_into_inner(wg).expect("into_inner"), 1);
        th.join().expect("join");
    }
}

#[test]
fn test_waitgroup_concurrent_wait_async() {
    // WaitGroup is !Sync, but its futures are Send, so two waiters may set the waker concurrently.
    let round = ROUND / 10;
    for _ in 0..round {
        let wg = WaitGroup::new((), 0);
        let guards: Vec<_> = (0..4).map(|_| wg.add_guard()).collect();
        let f1 = wg.wait_async();
        let f2 = wg.wait_async();
        thread::scope(|s| {
            for f in [f1, f2] {
                s.spawn(move || {
                    let waker = noop_waker();
                    let mut ctx = Context::from_waker(&waker);
                    let mut f = pin!(f);
                    while f.as_mut().poll(&mut ctx).is_pending() {
                        thread::yield_now();
                    }
                });
            }
            s.spawn(move || {
                for g in guards {
                    thread::yield_now();
                    drop(g);
                }
            });
        });
        assert_eq!(wg.get_left_seqcst(), 0);
    }
}

#[test]
fn test_multiplex_without_channel() {
    // Receiving from a Multiplex with no channel added must not index into an empty list.
    let mp = Multiplex::<mpsc::Array<usize>>::new();
    assert_eq!(mp.try_recv(), Err(TryRecvError::Disconnected));
    assert_eq!(mp.recv_timeout(Duration::from_millis(10)), Err(RecvTimeoutError::Disconnected));
    assert_eq!(mp.recv(), Err(RecvError));
}
