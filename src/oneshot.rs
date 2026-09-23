//! OneShot channel support both thread and async
//!
//! NOTE: In order to reduce initialization and teardown cost, this module use specialized sender [TxOneshot] and
//! receiver [RxOneshot] types.
//!
//! # Examples
//!
//! ## Thread Context
//!
//! ```
//! use crossfire::oneshot::oneshot;
//!
//! let (tx, rx) = oneshot();
//!
//! std::thread::spawn(move || {
//!     tx.send("Hello from sender!");
//! });
//!
//! let received = rx.recv().unwrap();
//! assert_eq!(received, "Hello from sender!");
//! ```
//!
//! ## Async Context
//!
//! ```
//! use crossfire::oneshot::oneshot;
//!
//! async fn example() {
//!     let (tx, rx) = oneshot();
//!
//!     tokio::spawn(async move {
//!         tx.send("Hello from async sender!");
//!     });
//!
//!     let received = rx.await.unwrap();
//!     assert_eq!(received, "Hello from async sender!");
//! }
//! ```

use crate::backoff::Backoff;
use crate::shared::*;
#[allow(unused_imports)]
use crate::{tokio_task_id, trace_log};
use core::cell::UnsafeCell;
use pin_project_lite::pin_project;
use std::future::{poll_fn, Future};
use std::pin::Pin;
use std::ptr::NonNull;
use std::sync::atomic::{
    fence, AtomicU8,
    Ordering::{self, AcqRel, Acquire, SeqCst},
};
use std::task::{Context, Poll, Waker};
use std::thread;
use std::time::{Duration, Instant};

/// Send/TxOneshot::drop will set this flag once, never changed.
const LOCK_FLAG: u8 = 0x1;
/// set by RxOneshot
const WAKER_SET_FLAG: u8 = 0x2;
/// set by any of TxOneshot/RxOneshot if it exit
const CLOSE_FLAG: u8 = 0x4;
const EXIST_FLAG: u8 = 0x8;
/// set by TxOneshot::poll_closed() while `tx_waker` holds its waker, only the sender writes
/// `tx_waker`, and only while this flag is clear and CLOSE_FLAG is not set.
const TX_WAKER_FLAG: u8 = 0x10;
/// set by RxOneshot together with CLOSE_FLAG while it wakes `tx_waker`,
/// the sender must not free the inner meanwhile.
const RX_WAKING_FLAG: u8 = 0x20;

struct OneShotInner<T> {
    state: AtomicU8,
    value: UnsafeCell<Option<T>>,
    o_waker: UnsafeCell<Option<ThinWaker>>,
    tx_waker: UnsafeCell<Option<Waker>>,
}

unsafe impl<T: Send> Send for OneShotInner<T> {}
unsafe impl<T: Send> Sync for OneShotInner<T> {}

impl<T> OneShotInner<T> {
    #[inline]
    fn new() -> Box<Self> {
        Box::new(Self {
            value: UnsafeCell::new(None),
            state: AtomicU8::new(0),
            o_waker: UnsafeCell::new(None),
            tx_waker: UnsafeCell::new(None),
        })
    }

    /// Both sides may read the receiver waker concurrently, the receiver writes it only while
    /// WAKER_SET_FLAG is clear, when the sender does not read it.
    #[inline]
    fn get_waker(&self) -> &Option<ThinWaker> {
        unsafe { &*self.o_waker.get() }
    }

    #[inline]
    #[allow(clippy::mut_from_ref)]
    fn get_waker_mut(&self) -> &mut Option<ThinWaker> {
        unsafe { &mut *self.o_waker.get() }
    }

    #[inline]
    fn tx_waker(&self) -> &Option<Waker> {
        unsafe { &*self.tx_waker.get() }
    }

    #[inline]
    #[allow(clippy::mut_from_ref)]
    fn tx_waker_mut(&self) -> &mut Option<Waker> {
        unsafe { &mut *self.tx_waker.get() }
    }

    #[inline(always)]
    fn value_mut(&self) -> &mut Option<T> {
        unsafe { &mut *self.value.get() }
    }

    #[inline(always)]
    fn _try_recv(&self, order: Ordering) -> Result<u8, u8> {
        let state = self.state.load(order);
        if state & LOCK_FLAG > 0 {
            Ok(state)
        } else {
            Err(state)
        }
    }

    // NOTE: in order to avoid miri borrow checker, use raw ptr here
    #[inline(always)]
    fn _consume_value(p: NonNull<Self>, mut state: u8) -> Option<T> {
        debug_assert!(
            state & LOCK_FLAG > 0,
            "oneshot:({:?}) consume value unexpected {state}",
            tokio_task_id!()
        );
        let this = unsafe { p.as_ref() };
        let item = if state & EXIST_FLAG > 0 { this.value_mut().take() } else { None };
        loop {
            if state & CLOSE_FLAG > 0 {
                trace_log!(
                    "oneshot:({:?}) recv value={} & destroy",
                    tokio_task_id!(),
                    item.is_some()
                );
                fence(Acquire);
                let _ = unsafe { Box::from_raw(p.as_ptr()) };
                // they close first
                return item;
            }
            if let Err(s) = this.state.compare_exchange(state, CLOSE_FLAG | state, AcqRel, Acquire)
            {
                trace_log!(
                    "oneshot:({:?}) recv value={} {state} close retry",
                    tokio_task_id!(),
                    item.is_some()
                );
                state = s;
            } else {
                trace_log!(
                    "oneshot:({:?}) recv value={} {state}",
                    tokio_task_id!(),
                    item.is_some()
                );
                // we close first
                return item;
            }
        }
    }

    /// return true to destroy
    #[inline(always)]
    fn _notify_rx(p: NonNull<Self>, exist: bool) -> bool {
        let this = unsafe { p.as_ref() };
        let mut old_state = 0;
        let exist_flag: u8 = if exist { EXIST_FLAG } else { 0 };
        loop {
            if old_state & CLOSE_FLAG > 0 {
                if old_state & RX_WAKING_FLAG > 0 {
                    // rx is waking our poll_closed() waker, leave the cleanup to it
                    match this.state.compare_exchange_weak(
                        old_state,
                        old_state | LOCK_FLAG,
                        AcqRel,
                        Acquire,
                    ) {
                        Ok(_) => return false,
                        Err(s) => {
                            old_state = s;
                            continue;
                        }
                    }
                }
                // WAKER_SET_FLAG | CLOSE_FLAG, or just CLOSE_FLAG
                trace_log!("oneshot:({:?}) rx closed", tokio_task_id!());
                return true;
            }
            let rx_state = old_state & !TX_WAKER_FLAG;
            let new_state = if rx_state == 0 {
                old_state | LOCK_FLAG | CLOSE_FLAG | exist_flag
            } else if rx_state == WAKER_SET_FLAG {
                old_state | LOCK_FLAG | exist_flag
            } else {
                panic!("unexpected state {}", old_state);
            };
            match this.state.compare_exchange_weak(old_state, new_state, AcqRel, Acquire) {
                Ok(_) => {
                    if rx_state == 0 {
                        trace_log!("oneshot:({:?}) send value", tokio_task_id!());
                        return false;
                    } else {
                        if let Some(waker) = this.get_waker().as_ref() {
                            // the sender should never move the waker, because rx::poll will
                            // validate it.
                            trace_log!("oneshot:({:?}) wake rx", tokio_task_id!());
                            waker.wake_by_ref();
                        } else {
                            unreachable!();
                        }
                        // rx does not set RX_WAKING_FLAG once LOCK_FLAG is set, so a failure
                        // here means rx closed.
                        if let Err(state) = this.state.compare_exchange(
                            new_state,
                            (new_state & !WAKER_SET_FLAG) | CLOSE_FLAG,
                            AcqRel,
                            Acquire,
                        ) {
                            // Safety: although we have no use for fail value other than debug log,
                            // but consider use failure ordering Acquire instead of Relaxed for miri,
                            // as a fence (stop the following from_raw to re-ordering).
                            debug_assert!(state & CLOSE_FLAG > 0, "unexpected state {state}");
                            trace_log!("oneshot:({:?}) rx closed {state}", tokio_task_id!());
                            return true;
                        } else {
                            // we close first, let rx do the cleanup
                            return false;
                        }
                    }
                }
                Err(s) => {
                    old_state = s;
                }
            }
        }
    }

    /// Close from the receiver side. If the sender is still alive and waits in poll_closed(),
    /// wake it.
    ///
    /// Return true when the sender is already done, so the caller must free the inner.
    #[inline(always)]
    fn _rx_close(p: NonNull<Self>) -> bool {
        let this = unsafe { p.as_ref() };
        let mut state = this.state.load(Acquire);
        loop {
            let wake_tx = Self::_tx_waiting(state);
            let new_state = state | CLOSE_FLAG | if wake_tx { RX_WAKING_FLAG } else { 0 };
            match this.state.compare_exchange_weak(state, new_state, AcqRel, Acquire) {
                Ok(_) => {
                    if state & CLOSE_FLAG > 0 {
                        // tx closed first
                        return true;
                    }
                    return wake_tx && Self::_wake_tx(p);
                }
                Err(s) => state = s,
            }
        }
    }

    /// Whether the sender has not sent nor dropped, and has a poll_closed() waker.
    #[inline(always)]
    fn _tx_waiting(state: u8) -> bool {
        state & (LOCK_FLAG | CLOSE_FLAG) == 0 && state & TX_WAKER_FLAG > 0
    }

    /// Called by rx after setting CLOSE_FLAG | RX_WAKING_FLAG.
    ///
    /// Return true when the sender finished meanwhile, so the caller must free the inner.
    #[inline(always)]
    fn _wake_tx(p: NonNull<Self>) -> bool {
        let this = unsafe { p.as_ref() };
        // The sender only reads tx_waker once CLOSE_FLAG is set, and RX_WAKING_FLAG keeps it
        // from freeing the inner.
        if let Some(waker) = this.tx_waker().as_ref() {
            waker.wake_by_ref();
        }
        let old = this.state.fetch_and(!RX_WAKING_FLAG, AcqRel);
        old & LOCK_FLAG > 0
    }

    #[inline(always)]
    fn set_waker(&self, waker: ThinWaker) -> Result<(), u8> {
        // thread context only need set waker once.
        // NOTE we should guarantee waker not set twice
        // (the recv_timeout API should not allow recv twice),
        // it will complicate things (like async poll).
        self.get_waker_mut().replace(waker);
        let mut state = 0;
        loop {
            match self.state.compare_exchange(state, state | WAKER_SET_FLAG, AcqRel, Acquire) {
                Ok(_) => return Ok(()),
                Err(s) => {
                    if s & !TX_WAKER_FLAG != 0 {
                        return Err(s);
                    }
                    state = s;
                }
            }
        }
    }

    /// With `abandon`, rx gives up and closes.
    ///
    /// Return Ok(true) when the caller must free the inner.
    #[inline(always)]
    fn cancel_waker(p: NonNull<Self>, abandon: bool) -> Result<bool, u8> {
        let this = unsafe { p.as_ref() };
        let mut state = this.state.load(Acquire);
        loop {
            if state & !TX_WAKER_FLAG != WAKER_SET_FLAG {
                // expect LOCK_FLAG | CLOSE_FLAG, or LOCK_FLAG | WAKER_SET_FLAG
                return Err(state);
            }
            let wake_tx = abandon && Self::_tx_waiting(state);
            let mut new_state = state & !WAKER_SET_FLAG;
            if abandon {
                new_state |= CLOSE_FLAG;
            }
            if wake_tx {
                new_state |= RX_WAKING_FLAG;
            }
            match this.state.compare_exchange(state, new_state, AcqRel, Acquire) {
                Ok(_) => return Ok(wake_tx && Self::_wake_tx(p)),
                Err(s) => state = s,
            }
        }
    }

    #[inline(always)]
    fn is_empty(&self) -> bool {
        let state = self.state.load(Ordering::SeqCst);
        state & EXIST_FLAG == 0
    }
}

/// Sender for oneshot channel
pub struct TxOneshot<T>(NonNull<OneShotInner<T>>);

unsafe impl<T: Send> Send for TxOneshot<T> {}
unsafe impl<T: Send> Sync for TxOneshot<T> {}

impl<T> TxOneshot<T> {
    /// Sending the item is one-time non-blocking behavior
    #[inline]
    pub fn send(self, item: T) {
        unsafe { self.0.as_ref() }.value_mut().replace(item);
        if OneShotInner::_notify_rx(self.0, true) {
            // drop inner
            let _ = unsafe { Box::from_raw(self.0.as_ptr()) };
        }
        std::mem::forget(self);
    }

    /// return true when RxOneshot is dropped
    ///
    /// # Safety
    ///
    /// This is not SeqCst, only Acquire, for sender we don't require to know immediately.
    #[inline]
    pub fn is_disconnected(&self) -> bool {
        unsafe { self.0.as_ref() }.state.load(Acquire) & CLOSE_FLAG > 0
    }

    /// Poll whether the [RxOneshot] is dropped (or gave up waiting), registering the task to
    /// be woken when it is.
    ///
    /// Only the waker of the most recent call is woken.
    pub fn poll_closed(&mut self, ctx: &mut Context) -> Poll<()> {
        let inner = unsafe { self.0.as_ref() };
        let mut state = inner.state.load(Acquire);
        if state & CLOSE_FLAG > 0 {
            return Poll::Ready(());
        }
        if state & TX_WAKER_FLAG > 0 {
            // rx only reads tx_waker, so reading it while the flag is set is fine.
            if inner.tx_waker().as_ref().is_some_and(|w| w.will_wake(ctx.waker())) {
                return Poll::Pending;
            }
            // Clear the flag to take tx_waker back before replacing it,
            // this fails once rx closed and may be reading it.
            loop {
                match inner.state.compare_exchange_weak(
                    state,
                    state & !TX_WAKER_FLAG,
                    AcqRel,
                    Acquire,
                ) {
                    Ok(_) => break,
                    Err(s) => {
                        if s & CLOSE_FLAG > 0 {
                            return Poll::Ready(());
                        }
                        state = s;
                    }
                }
            }
        }
        // rx does not read tx_waker while TX_WAKER_FLAG is clear
        inner.tx_waker_mut().replace(ctx.waker().clone());
        if inner.state.fetch_or(TX_WAKER_FLAG, AcqRel) & CLOSE_FLAG > 0 {
            // rx closed before seeing the flag, and will not wake us
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    }

    /// Wait until the [RxOneshot] is dropped (or gave up waiting).
    #[inline]
    pub async fn closed(&mut self) {
        poll_fn(|ctx| self.poll_closed(ctx)).await
    }
}

impl<T> Drop for TxOneshot<T> {
    #[inline]
    fn drop(&mut self) {
        if OneShotInner::_notify_rx(self.0, false) {
            // drop inner
            let _ = unsafe { Box::from_raw(self.0.as_ptr()) };
        }
    }
}

/// Receiver for oneshot channel
#[must_use]
pub struct RxOneshot<T>(Option<NonNull<OneShotInner<T>>>);

unsafe impl<T: Send> Send for RxOneshot<T> {}

impl<T> Drop for RxOneshot<T> {
    #[inline]
    fn drop(&mut self) {
        if let Some(p) = self.0 {
            if OneShotInner::_rx_close(p) {
                trace_log!("oneshot:({:?}) rx drop destroy", tokio_task_id!());
                let _ = unsafe { Box::from_raw(p.as_ptr()) };
            } else {
                // let tx do the cleanup
                trace_log!("oneshot:({:?}) rx drop", tokio_task_id!());
            }
        }
    }
}

impl<T> RxOneshot<T> {
    /// NOTE: this will blocking current thread
    #[inline]
    pub fn recv(self) -> Result<T, RecvError> {
        if let Ok(item) = self._recv_blocking(None) {
            return Ok(item);
        }
        Err(RecvError)
    }

    /// NOTE: this will blocking current thread with a timeout
    #[inline]
    pub fn recv_timeout(self, timeout: Duration) -> Result<T, RecvTimeoutError> {
        let deadline = Instant::now() + timeout;
        match self._recv_blocking(Some(deadline)) {
            Ok(item) => Ok(item),
            Err(true) => Err(RecvTimeoutError::Timeout),
            Err(false) => Err(RecvTimeoutError::Disconnected),
        }
    }

    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        if let Some(p) = self.0.as_ref() {
            let inner = unsafe { p.as_ref() };
            inner.is_empty()
        } else {
            true
        }
    }

    #[inline]
    pub fn try_recv(&mut self) -> Result<T, TryRecvError> {
        if let Some(p) = self.0.as_ref() {
            let p = *p;
            if let Ok(state) = unsafe { p.as_ref() }._try_recv(Acquire) {
                self.0 = None;
                if let Some(item) = OneShotInner::_consume_value(p, state) {
                    return Ok(item);
                } else {
                    return Err(TryRecvError::Disconnected);
                }
            } else {
                Err(TryRecvError::Empty)
            }
        } else {
            Err(TryRecvError::Disconnected)
        }
    }

    #[inline]
    pub async fn recv_async(self) -> Result<T, RecvError> {
        self.await
    }

    #[inline]
    fn poll(&mut self, ctx: &mut Context<'_>) -> Poll<Result<T, ()>> {
        let p: NonNull<OneShotInner<T>> = if let Some(p) = self.0.as_ref() {
            *p
        } else {
            // might poll after try_recv() finish
            return Poll::Ready(Err(()));
        };
        let inner = unsafe { p.as_ref() };
        macro_rules! process {
            ($state: expr) => {
                self.0 = None;
                if let Some(item) = OneShotInner::_consume_value(p, $state) {
                    return Poll::Ready(Ok(item));
                } else {
                    return Poll::Ready(Err(()));
                }
            };
        }
        macro_rules! check_exist {
            ($order: expr) => {{
                match inner._try_recv($order) {
                    Ok(state) => {
                        process!(state);
                    }
                    Err(s) => s,
                }
            }};
        }
        let state = check_exist!(SeqCst);
        if state & WAKER_SET_FLAG > 0 {
            let waker = inner.get_waker().as_ref().unwrap();
            if waker.will_wake(ctx) {
                trace_log!("oneshot:({:?}) spurious waked state {}", tokio_task_id!(), state,);
                return Poll::Pending;
            }
            if let Err(state) = OneShotInner::cancel_waker(p, false) {
                process!(state);
            }
        }
        if let Err(state) = inner.set_waker(ThinWaker::Async(ctx.waker().clone())) {
            process!(state);
        }
        Poll::Pending
    }

    /// On Disconnected return Err(false),
    /// Err(true) when timeout.
    #[inline(always)]
    pub(crate) fn _recv_blocking(self, deadline: Option<Instant>) -> Result<T, bool> {
        let p: NonNull<OneShotInner<T>> = if let Some(p) = self.0.as_ref() {
            *p
        } else {
            // might recv() after try_recv() ok/disconnect
            return Err(false);
        };
        let inner = unsafe { p.as_ref() };
        macro_rules! process {
            ($state: expr) => {
                let _ = inner;
                std::mem::forget(self);
                if let Some(item) = OneShotInner::_consume_value(p, $state) {
                    return Ok(item);
                } else {
                    return Err(false);
                }
            };
        }
        macro_rules! try_recv {
            ($order: expr) => {
                if let Ok(state) = inner._try_recv($order) {
                    trace_log!("try_recv got {state}");
                    process!(state);
                }
            };
        }
        try_recv!(Acquire);
        let mut backoff = Backoff::new();
        while !backoff.snooze() {
            try_recv!(Acquire);
        }
        if let Err(state) = inner.set_waker(ThinWaker::Blocking(thread::current())) {
            process!(state);
        }
        trace_log!("oneshot: waker set");
        loop {
            try_recv!(SeqCst);
            match check_timeout(deadline) {
                Ok(None) => {
                    std::thread::park();
                }
                Ok(Some(dur)) => {
                    std::thread::park_timeout(dur);
                }
                Err(_) => {
                    trace_log!("oneshot: to cancel_waker on timeout");
                    match OneShotInner::cancel_waker(p, true) {
                        Err(state) => {
                            process!(state);
                        }
                        Ok(destroy) => {
                            let _ = inner;
                            std::mem::forget(self);
                            if destroy {
                                // tx finished while we woke its poll_closed() waker
                                let _ = unsafe { Box::from_raw(p.as_ptr()) };
                            }
                            // otherwise we close first, tx does the cleanup
                            return Err(true);
                        }
                    }
                }
            }
        }
    }

    /// Wrap RxOneshot with timeout, consume self when it's done.
    /// The Future returns `Result<T, RecvTimeoutError>`
    #[cfg(any(feature = "tokio", feature = "async_std"))]
    #[cfg_attr(docsrs, doc(cfg(any(feature = "tokio", feature = "async_std"))))]
    #[inline]
    pub async fn recv_async_timeout(
        self, timeout: std::time::Duration,
    ) -> Result<T, RecvTimeoutError> {
        #[cfg(feature = "tokio")]
        {
            let sleep = tokio::time::sleep(timeout);
            self.recv_async_with_timer(sleep).await
        }
        #[cfg(feature = "async_std")]
        {
            let sleep = async_std::task::sleep(timeout);
            self.recv_async_with_timer(sleep).await
        }
    }

    /// Wrap RxOneshot with custom sleep function, consume self when it's done.
    ///
    /// The behavior is atomic: the message is either received successfully or the operation is canceled due to a timeout.
    ///
    /// Returns `Ok(T)` when successful.
    ///
    /// Returns Err([RecvTimeoutError::Timeout]) when a message could not be received because the channel is empty and the operation timed out.
    ///
    /// Returns Err([RecvTimeoutError::Disconnected]) if the sender has been dropped and the channel is empty.
    ///
    /// # Argument:
    ///
    /// * `sleep`: The sleep function. the return value of `sleep` is ignore. We add generic `R` just in order to support smol::Timer
    /// # Example
    ///
    /// Example with smol
    ///
    /// ```rust
    /// extern crate smol;
    /// use std::time::Duration;
    /// use crossfire::*;
    /// async fn foo() {
    ///     let (tx, rx) = oneshot::oneshot::<usize>();
    ///     match rx.recv_async_with_timer(smol::Timer::after(Duration::from_secs(1))).await {
    ///         Ok(_item)=>{
    ///             println!("message recv");
    ///         }
    ///         Err(RecvTimeoutError::Timeout)=>{
    ///             println!("timeout");
    ///         }
    ///         Err(RecvTimeoutError::Disconnected)=>{
    ///             println!("sender-side closed");
    ///         }
    ///     }
    /// }
    /// ```
    ///
    /// Example with tokio:
    ///
    /// ```rust
    /// use std::time::Duration;
    /// use crossfire::*;
    /// async fn foo() {
    ///     let (tx, rx) = oneshot::oneshot::<usize>();
    ///     let sleep = tokio::time::sleep(Duration::from_secs(1));
    ///     let _r = rx.recv_async_with_timer(sleep).await;
    /// }
    /// ```
    #[inline]
    pub fn recv_async_with_timer<F, R>(self, sleep: F) -> OneshotTimeoutFuture<T, F, R>
    where
        F: Future<Output = R>,
    {
        OneshotTimeoutFuture { rx: self, sleep }
    }
}

impl<T> Future for RxOneshot<T> {
    type Output = Result<T, RecvError>;

    #[inline]
    fn poll(self: Pin<&mut Self>, ctx: &mut Context) -> Poll<Self::Output> {
        let this = self.get_mut();
        match this.poll(ctx) {
            Poll::Ready(Ok(item)) => Poll::Ready(Ok(item)),
            Poll::Ready(Err(())) => Poll::Ready(Err(RecvError)),
            Poll::Pending => Poll::Pending,
        }
    }
}

pin_project! {
    pub struct OneshotTimeoutFuture<T, F, R>
    where
        F: Future<Output = R>,
    {
        rx: RxOneshot<T>,
        #[pin]
        sleep: F,
    }
}

impl<T, F, R> Future for OneshotTimeoutFuture<T, F, R>
where
    F: Future<Output = R>,
{
    type Output = Result<T, RecvTimeoutError>;

    #[inline]
    fn poll(self: Pin<&mut Self>, ctx: &mut Context) -> Poll<Self::Output> {
        let this = self.project();
        match this.rx.poll(ctx) {
            Poll::Ready(Ok(item)) => return Poll::Ready(Ok(item)),
            Poll::Ready(Err(())) => return Poll::Ready(Err(RecvTimeoutError::Disconnected)),
            _ => {}
        }
        if this.sleep.poll(ctx).is_ready() {
            Poll::Ready(Err(RecvTimeoutError::Timeout))
        } else {
            Poll::Pending
        }
    }
}

#[inline]
pub fn oneshot<T>() -> (TxOneshot<T>, RxOneshot<T>) {
    let p = NonNull::from(Box::leak(OneShotInner::new()));
    let tx = TxOneshot(p);
    let rx = RxOneshot(Some(p));
    (tx, rx)
}
