use crate::*;
use captains_log::logfn;
use crossfire::*;
use fastrand;
use rstest::*;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::{Context, Wake, Waker};
use std::thread;
use std::time::Duration;

#[fixture]
fn setup_log() {
    _setup_log();
}

#[logfn]
#[rstest]
fn test_oneshot_blocking_basic(setup_log: ()) {
    let (tx, mut rx) = oneshot::oneshot();
    assert_eq!(rx.try_recv().unwrap_err(), TryRecvError::Empty);
    assert_eq!(rx.is_empty(), true);
    tx.send(42);
    assert_eq!(rx.is_empty(), false);
    assert_eq!(rx.recv(), Ok(42));

    let (tx, mut rx) = oneshot::oneshot();
    assert_eq!(rx.try_recv().unwrap_err(), TryRecvError::Empty);
    tx.send(41);
    assert_eq!(rx.try_recv(), Ok(41));
    assert_eq!(rx.try_recv().unwrap_err(), TryRecvError::Disconnected);
    assert_eq!(rx.recv().unwrap_err(), RecvError);
}

#[logfn]
#[rstest]
fn test_oneshot_blocking_drop_tx(setup_log: ()) {
    let (tx, rx) = oneshot::oneshot::<i32>();
    drop(tx);
    assert_eq!(rx.recv(), Err(RecvError));

    let (tx, rx) = oneshot::oneshot::<i32>();
    let th = thread::spawn(move || {
        // Should be wake up on sender drop
        assert_eq!(rx.recv(), Err(RecvError));
    });
    thread::sleep(Duration::from_millis(fastrand::u64(1..=500)));
    drop(tx);
    th.join().expect("join");
}

#[logfn]
#[rstest]
fn test_oneshot_blocking_drop_rx(setup_log: ()) {
    let (tx, rx) = oneshot::oneshot::<i32>();
    drop(rx);
    assert!(tx.is_disconnected());
    // send consumes tx, returns ()
    tx.send(42);
}

#[logfn]
#[rstest]
fn test_oneshot_blocking_leak(setup_log: ()) {
    // Check if OneShot drops the value if not received
    reset_drop_counter();
    {
        let (tx, _rx) = oneshot::oneshot::<SmallMsg>();
        tx.send(SmallMsg::new(1));
    } // tx dropped (closed), rx dropped (OneShot dropped). msg should be dropped.
    assert_eq!(get_drop_counter(), 1);
}

#[logfn]
#[rstest]
fn test_oneshot_blocking_drop_after_recv(setup_log: ()) {
    // Check if OneShot drops the value after recv (it shouldn't, Rx has it)
    reset_drop_counter();
    {
        let (tx, rx) = oneshot::oneshot::<SmallMsg>();
        tx.send(SmallMsg::new(1));
        let msg = rx.recv().unwrap();
        assert_eq!(get_drop_counter(), 0);
        drop(msg);
        assert_eq!(get_drop_counter(), 1);
    }
    // OneShot dropped. Should NOT drop again.
    assert_eq!(get_drop_counter(), 1);
}

#[logfn]
#[rstest]
fn test_oneshot_async_basic(setup_log: ()) {
    runtime_block_on!(async move {
        let (tx, mut rx) = oneshot::oneshot();
        assert_eq!(rx.try_recv().unwrap_err(), TryRecvError::Empty);
        assert_eq!(rx.is_empty(), true);
        tx.send(42);
        assert_eq!(rx.is_empty(), false);
        assert_eq!(rx.await, Ok(42));
        let (tx, mut rx) = oneshot::oneshot();
        assert_eq!(rx.try_recv().unwrap_err(), TryRecvError::Empty);
        tx.send(41);
        assert_eq!(rx.try_recv(), Ok(41));
        assert_eq!(rx.try_recv().unwrap_err(), TryRecvError::Disconnected);
        assert_eq!(rx.await.unwrap_err(), RecvError);
    });
}

#[cfg(feature = "time")]
#[logfn]
#[rstest]
fn test_oneshot_async_drop_tx(setup_log: ()) {
    runtime_block_on!(async move {
        let (tx, rx) = oneshot::oneshot::<i32>();
        drop(tx);
        assert_eq!(rx.await, Err(RecvError));
        log::debug!("next test");
        let (tx, rx) = oneshot::oneshot::<i32>();
        let th = async_spawn!(async move {
            // Should be wake up on sender drop
            assert_eq!(rx.await, Err(RecvError));
        });
        sleep(Duration::from_millis(fastrand::u64(1..=500))).await;
        drop(tx);
        let _ = async_join_result!(th);
    });
}

#[logfn]
#[rstest]
fn test_oneshot_async_pressure(setup_log: ()) {
    let count = {
        #[cfg(miri)]
        {
            10usize
        }
        #[cfg(not(miri))]
        {
            100usize
        }
    };
    runtime_block_on!(async move {
        let mut tasks = Vec::new();
        for i in 0..count {
            tasks.push(async_spawn!(async move {
                let (tx, rx) = oneshot::oneshot();
                tx.send(i);
                assert_eq!(rx.await, Ok(i));
            }));
        }
        for t in tasks {
            let _ = async_join_result!(t);
        }
    });
}

#[logfn]
#[rstest]
fn test_oneshot_blocking_batch(setup_log: ()) {
    let mut txs = Vec::with_capacity(ROUND);
    let mut rxs = Vec::with_capacity(ROUND);
    for _i in 0..ROUND {
        let (tx, rx) = oneshot::oneshot();
        txs.push(tx);
        rxs.push(rx);
    }
    let th = thread::spawn(move || {
        for (i, tx) in txs.into_iter().enumerate() {
            tx.send(i);
        }
    });
    for (i, rx) in rxs.into_iter().enumerate() {
        assert_eq!(rx.recv(), Ok(i));
    }
    th.join().unwrap();
}

#[logfn]
#[rstest]
fn test_oneshot_async_batch(setup_log: ()) {
    runtime_block_on!(async move {
        let mut txs = Vec::with_capacity(ROUND);
        let mut rxs = Vec::with_capacity(ROUND);
        for _i in 0..ROUND {
            let (tx, rx) = oneshot::oneshot();
            txs.push(tx);
            rxs.push(rx);
        }
        let th = async_spawn!(async move {
            for (i, tx) in txs.into_iter().enumerate() {
                tx.send(i);
            }
        });
        for (i, rx) in rxs.into_iter().enumerate() {
            assert_eq!(rx.await, Ok(i));
        }
        async_join_result!(th);
    });
}

#[logfn]
#[rstest]
fn test_oneshot_blocking_concurrent(setup_log: ()) {
    let count = {
        #[cfg(miri)]
        {
            10usize
        }
        #[cfg(not(miri))]
        {
            50usize
        }
    };
    let mut th_s = Vec::new();
    for i in 0..count {
        let (tx, rx) = oneshot::oneshot();
        th_s.push(thread::spawn(move || {
            tx.send(i);
        }));
        th_s.push(thread::spawn(move || {
            assert_eq!(rx.recv(), Ok(i));
        }));
    }
    for th in th_s {
        th.join().unwrap();
    }
}

#[logfn]
#[rstest]
fn test_oneshot_async_concurrent(setup_log: ()) {
    let count = {
        #[cfg(miri)]
        {
            10usize
        }
        #[cfg(not(miri))]
        {
            100usize
        }
    };
    runtime_block_on!(async move {
        let mut tasks = Vec::new();
        for i in 0..count {
            let (tx, rx) = oneshot::oneshot();
            tasks.push(async_spawn!(async move {
                tx.send(i);
            }));
            tasks.push(async_spawn!(async move {
                assert_eq!(rx.await, Ok(i));
            }));
        }
        for t in tasks {
            let _ = async_join_result!(t);
        }
    });
}

#[logfn]
#[rstest]
fn test_oneshot_blocking_with_sleep(setup_log: ()) {
    #[cfg(miri)]
    {
        // sleep in miri will be too slow
        println!("skip on miri");
        return;
    }
    #[cfg(not(miri))]
    {
        let count = 50usize;
        let mut th_s = Vec::new();
        for i in 0..(count as u64) {
            th_s.push(thread::spawn(move || {
                let (tx, rx) = oneshot::oneshot();
                // Spawn a thread that sends after a short delay
                thread::spawn(move || {
                    thread::sleep(Duration::from_millis(i % 10)); // Vary the delay
                    tx.send(i);
                });
                // Wait for the value
                assert_eq!(rx.recv(), Ok(i));
            }));
        }
        for th in th_s {
            th.join().unwrap();
        }
    }
}

#[cfg(feature = "time")]
#[logfn]
#[rstest]
fn test_oneshot_async_with_sleep(setup_log: ()) {
    #[cfg(miri)]
    {
        // sleep in miri will be too slow
        println!("skip on miri");
    }
    #[cfg(not(miri))]
    {
        let count = 50usize;
        runtime_block_on!(async move {
            let mut tasks = Vec::new();
            for i in 0..count {
                tasks.push(async_spawn!(async move {
                    let (tx, rx) = oneshot::oneshot();
                    let th = async_spawn!(async move {
                        sleep(Duration::from_millis((i % 10) as u64)).await;
                        tx.send(i);
                    });

                    // Wait for the value
                    assert_eq!(rx.await, Ok(i));
                    let _ = async_join_result!(th);
                }));
            }
            for t in tasks {
                let _ = async_join_result!(t);
            }
        });
    }
}

#[cfg(feature = "time")]
#[logfn]
#[rstest]
fn test_oneshot_async_batch_with_interval(setup_log: ()) {
    #[cfg(miri)]
    {
        // sleep in miri will be too slow
        println!("skip on miri");
        return;
    }
    #[cfg(not(miri))]
    {
        let batch_size = 30;
        runtime_block_on!(async move {
            let mut tasks = Vec::new();

            // Create a batch of oneshots
            for i in 0..batch_size {
                tasks.push(async_spawn!(async move {
                    let (tx, rx) = oneshot::oneshot();
                    let th = async_spawn!(async move {
                        // Sleep for different durations based on index
                        sleep(Duration::from_millis((i * 2) as u64)).await;
                        tx.send(i);
                    });

                    // Wait for the value
                    assert_eq!(rx.await, Ok(i));
                    let _ = async_join_result!(th);
                }));
            }
            for t in tasks {
                let _ = async_join_result!(t);
            }
        });
    }
}

#[cfg(feature = "time")]
#[logfn]
#[rstest]
fn test_oneshot_blocking_timeout_fail(setup_log: ()) {
    let (_tx, rx) = oneshot::oneshot::<i32>();
    let start = std::time::Instant::now();
    let res = rx.recv_timeout(Duration::from_millis(100));
    assert_eq!(res, Err(RecvTimeoutError::Timeout));
    assert!(start.elapsed() >= Duration::from_millis(100));
}

#[cfg(feature = "time")]
#[logfn]
#[rstest]
fn test_oneshot_blocking_timeout_success(setup_log: ()) {
    let (tx, rx) = oneshot::oneshot::<i32>();
    let th = thread::spawn(move || {
        thread::sleep(Duration::from_millis(50));
        tx.send(42);
    });
    let _res = rx.recv_timeout(Duration::from_secs(1));
    #[cfg(not(miri))]
    assert_eq!(_res, Ok(42));
    let _ = th.join();
}

#[cfg(feature = "time")]
#[logfn]
#[rstest]
fn test_oneshot_blocking_timeout_disconnected(setup_log: ()) {
    let (tx, rx) = oneshot::oneshot::<i32>();
    let th = thread::spawn(move || {
        thread::sleep(Duration::from_millis(50));
        drop(tx);
    });
    let _res = rx.recv_timeout(Duration::from_millis(200));
    let _ = th.join();
    assert!(_res.is_err());
    // might be timeout or disconnected
}

#[cfg(feature = "time")]
#[logfn]
#[rstest]
fn test_oneshot_async_timeout_fail(setup_log: ()) {
    runtime_block_on!(async move {
        let (_tx, rx) = oneshot::oneshot::<i32>();
        let start = std::time::Instant::now();
        let sleep_fut = sleep(Duration::from_millis(100));
        futures_util::pin_mut!(sleep_fut);
        let res = rx.recv_async_with_timer(sleep_fut).await;
        assert_eq!(res, Err(RecvTimeoutError::Timeout));
        assert!(start.elapsed() >= Duration::from_millis(100));
    });
}

#[cfg(feature = "time")]
#[logfn]
#[rstest]
fn test_oneshot_async_timeout_disconnected(setup_log: ()) {
    runtime_block_on!(async move {
        let (tx, rx) = oneshot::oneshot::<i32>();
        let th = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(50));
            drop(tx);
        });
        let _res = rx.recv_async_with_timer(sleep(Duration::from_secs(1))).await;
        let _ = th.join();
        #[cfg(not(miri))]
        assert_eq!(_res, Err(RecvTimeoutError::Disconnected));
    });
}

#[cfg(feature = "time")]
#[logfn]
#[rstest]
fn test_oneshot_async_timeout_success(setup_log: ()) {
    runtime_block_on!(async move {
        let (tx, rx) = oneshot::oneshot::<i32>();
        let th = async_spawn!(async move {
            sleep(Duration::from_millis(50)).await;
            tx.send(42);
        });
        let _res = rx.recv_async_with_timer(sleep(Duration::from_secs(2))).await;
        #[cfg(not(miri))]
        assert_eq!(_res, Ok(42));
        async_join_result!(th);
    });
}

struct CountWaker(AtomicUsize);

impl Wake for CountWaker {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

fn count_waker() -> (Arc<CountWaker>, Waker) {
    let w = Arc::new(CountWaker(AtomicUsize::new(0)));
    (w.clone(), Waker::from(w))
}

fn woken(w: &CountWaker) -> usize {
    w.0.load(Ordering::SeqCst)
}

#[logfn]
#[rstest]
fn test_oneshot_poll_closed(setup_log: ()) {
    let (counter, waker) = count_waker();
    let mut ctx = Context::from_waker(&waker);
    let item = Arc::new(());

    // rx dropped before polling
    let (mut tx, rx) = oneshot::oneshot::<Arc<()>>();
    drop(rx);
    assert!(tx.poll_closed(&mut ctx).is_ready());
    tx.send(item.clone());
    assert_eq!(Arc::strong_count(&item), 1);

    // rx dropped while registered, then tx sends
    let (mut tx, rx) = oneshot::oneshot::<Arc<()>>();
    assert!(tx.poll_closed(&mut ctx).is_pending());
    assert!(tx.poll_closed(&mut ctx).is_pending());
    assert_eq!(woken(&counter), 0);
    drop(rx);
    assert_eq!(woken(&counter), 1);
    assert!(tx.poll_closed(&mut ctx).is_ready());
    tx.send(item.clone());
    assert_eq!(Arc::strong_count(&item), 1);

    // Only the latest waker is woken
    let (counter2, waker2) = count_waker();
    let (mut tx, rx) = oneshot::oneshot::<Arc<()>>();
    assert!(tx.poll_closed(&mut ctx).is_pending());
    assert!(tx.poll_closed(&mut Context::from_waker(&waker2)).is_pending());
    drop(rx);
    assert_eq!(woken(&counter), 1);
    assert_eq!(woken(&counter2), 1);
    drop(tx);

    // Registered, then sends normally
    let (mut tx, rx) = oneshot::oneshot::<Arc<()>>();
    assert!(tx.poll_closed(&mut ctx).is_pending());
    tx.send(item.clone());
    assert_eq!(Arc::strong_count(&item), 2);
    drop(rx.recv().expect("recv"));
    assert_eq!(Arc::strong_count(&item), 1);

    // Registered, then drops
    let (mut tx, rx) = oneshot::oneshot::<Arc<()>>();
    assert!(tx.poll_closed(&mut ctx).is_pending());
    drop(tx);
    assert_eq!(rx.recv(), Err(RecvError));
    assert_eq!(woken(&counter), 1);

    // rx gives up on timeout
    let (mut tx, rx) = oneshot::oneshot::<Arc<()>>();
    assert!(tx.poll_closed(&mut ctx).is_pending());
    assert_eq!(rx.recv_timeout(Duration::from_millis(1)), Err(RecvTimeoutError::Timeout));
    assert_eq!(woken(&counter), 2);
    assert!(tx.poll_closed(&mut ctx).is_ready());
    tx.send(item.clone());
    assert_eq!(Arc::strong_count(&item), 1);

    // rx future polled then cancelled
    let (mut tx, mut rx) = oneshot::oneshot::<Arc<()>>();
    assert!(tx.poll_closed(&mut ctx).is_pending());
    assert!(Pin::new(&mut rx).poll(&mut ctx).is_pending());
    drop(rx);
    assert_eq!(woken(&counter), 3);
    assert!(tx.poll_closed(&mut ctx).is_ready());
    drop(tx);
}

#[logfn]
#[rstest]
fn test_oneshot_closed_async(setup_log: ()) {
    runtime_block_on!(async move {
        let (mut tx, rx) = oneshot::oneshot::<usize>();
        let th = async_spawn!(async move {
            sleep(Duration::from_millis(10)).await;
            drop(rx);
        });
        tx.closed().await;
        assert!(tx.is_disconnected());
        let _ = th.await;
    });
}

#[logfn]
#[rstest]
fn test_oneshot_poll_closed_concurrent(setup_log: ()) {
    // The last of tx and rx frees the channel, while rx may be waking tx's waker.
    let item = Arc::new(());
    for i in 0..ROUND {
        let (mut tx, rx) = oneshot::oneshot::<Arc<()>>();
        let value = item.clone();
        let th = thread::spawn(move || {
            let (_counter, waker) = count_waker();
            let mut ctx = Context::from_waker(&waker);
            let _ = tx.poll_closed(&mut ctx);
            match i % 3 {
                0 => tx.send(value),
                1 => drop(tx),
                _ => {
                    // Alternate wakers, so tx replaces its waker while rx may be closing
                    let (_counter2, waker2) = count_waker();
                    let mut ctx2 = Context::from_waker(&waker2);
                    loop {
                        if tx.poll_closed(&mut ctx2).is_ready()
                            || tx.poll_closed(&mut ctx).is_ready()
                        {
                            break;
                        }
                        thread::yield_now();
                    }
                    tx.send(value);
                }
            }
        });
        match i % 4 {
            0 => drop(rx),
            1 => drop(rx.recv_timeout(Duration::from_micros(1))),
            2 => {
                let (_counter, waker) = count_waker();
                let mut rx = rx;
                let _ = Pin::new(&mut rx).poll(&mut Context::from_waker(&waker));
                drop(rx);
            }
            _ => {
                if i % 3 == 2 {
                    drop(rx);
                } else {
                    drop(rx.recv());
                }
            }
        }
        th.join().expect("join");
    }
    assert_eq!(Arc::strong_count(&item), 1);
}
