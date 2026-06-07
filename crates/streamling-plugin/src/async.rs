#![allow(clippy::missing_const_for_thread_local)]

use std::cell::RefCell;
use std::sync::Arc;

use abi_stable::derive_macro_reexports::{RErr, ROk, RResult, TD_Opaque};
use abi_stable::sabi_trait;
use abi_stable::std_types::{RBox, RDuration};
use abi_stable::traits::IntoReprRust;
use async_ffi::{FfiFuture, FutureExt as AsyncFfiFutureExt};
use tokio::runtime::{EnterGuard, Handle, Runtime};
use tracing::info;

thread_local! {
    // The runtime will be unique for each plugin library
    static RUNTIME: RefCell<Option<Arc<Runtime>>> = RefCell::new(None);
    static RUNTIME_ENTER_GUARD: RefCell<Option<EnterGuard<'static>>> = RefCell::new(None);
}

/// Async runtime for performing asynchronous operations in the plugin.
/// See [`PluginTokioWrapper`] on the host side for FFI-safe implementation.
#[sabi_trait]
pub trait PluginAsyncRuntime: Send + Sync + Clone {
    fn spawn(&self, fut: FfiFuture<()>) -> FfiFuture<()>;
    fn sleep(&self, dur: RDuration) -> FfiFuture<()>;
    fn timeout(&self, dur: RDuration, fut: FfiFuture<()>) -> FfiFuture<RResult<(), ()>>;
    fn block_on(&self, fut: FfiFuture<()>);
    fn yield_now(&self) -> FfiFuture<()>;
}

pub type PluginAsyncRuntimeObj = PluginAsyncRuntime_TO<'static, RBox<()>>;

/// A special implementation of `PluginAsyncRuntime` that uses Tokio's runtime directly. This means
/// that a complete Tokio runtime is created and used by a plugin.
#[derive(Clone)]
pub struct DirectTokioProxy {
    handle: Handle,
}

impl DirectTokioProxy {
    pub fn new() -> Self {
        match Handle::try_current() {
            Ok(handle) => {
                info!("Using existing Tokio runtime");
                Self { handle }
            }
            Err(_) => {
                let existing_runtime = RUNTIME.with(|r| r.borrow().clone());

                if let Some(runtime) = existing_runtime {
                    info!("Using existing thread-local Tokio runtime");
                    let handle = runtime.handle().clone();
                    Self { handle }
                } else {
                    info!("No existing Tokio runtime found, creating new runtime");
                    let runtime = tokio::runtime::Builder::new_multi_thread()
                        // TODO: should this be configurable?
                        // .worker_threads(num_workers)
                        .enable_all()
                        .build()
                        .expect("Failed to create Tokio runtime");
                    let handle = runtime.handle().clone();

                    let runtime_arc = Arc::new(runtime);

                    let guard = unsafe {
                        // SAFETY: We're storing the guard in thread-local storage with the same lifetime as the runtime
                        std::mem::transmute::<EnterGuard<'_>, EnterGuard<'static>>(
                            runtime_arc.enter(),
                        )
                    };

                    RUNTIME.with(|r| {
                        *r.borrow_mut() = Some(runtime_arc);
                    });
                    RUNTIME_ENTER_GUARD.with(|g| {
                        *g.borrow_mut() = Some(guard);
                    });

                    Self { handle }
                }
            }
        }
    }

    pub fn into_async_runtime_obj(self) -> PluginAsyncRuntimeObj {
        PluginAsyncRuntime_TO::from_value(self, TD_Opaque)
    }
}

impl Default for DirectTokioProxy {
    fn default() -> Self {
        Self::new()
    }
}

impl PluginAsyncRuntime for DirectTokioProxy {
    fn spawn(&self, fut: FfiFuture<()>) -> FfiFuture<()> {
        let handle = self.handle.clone();
        async move {
            if let Err(e) = handle.spawn(fut).await {
                panic!("Spawned task panicked: {e:?}");
            }
        }
        .into_ffi()
    }

    fn sleep(&self, dur: RDuration) -> FfiFuture<()> {
        async move { tokio::time::sleep(dur.into_rust()).await }.into_ffi()
    }

    fn timeout(&self, dur: RDuration, fut: FfiFuture<()>) -> FfiFuture<RResult<(), ()>> {
        async move {
            match tokio::time::timeout(dur.into_rust(), fut).await {
                Ok(_) => ROk(()),
                Err(_) => RErr(()),
            }
        }
        .into_ffi()
    }

    fn block_on(&self, fut: FfiFuture<()>) {
        self.handle.block_on(fut);
    }

    fn yield_now(&self) -> FfiFuture<()> {
        tokio::task::yield_now().into_ffi()
    }
}
