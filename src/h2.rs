use pyo3::prelude::*;
use std::sync::Arc;

static H2_RUNTIME: std::sync::OnceLock<tokio::runtime::Runtime> = std::sync::OnceLock::new();

fn get_h2_runtime() -> &'static tokio::runtime::Runtime {
    H2_RUNTIME.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .thread_name("multiloop-h2-worker")
            .build()
            .expect("Failed to create tokio runtime for H2")
    })
}

pub(crate) type H2ResponseChunk = (u16, Vec<(Vec<u8>, Vec<u8>)>, Vec<u8>, bool);

#[pyclass(module = "multiloop._multiloop_core")]
pub struct PyH2Bridge {
    req_rx: flume::Receiver<Option<Vec<u8>>>,
    resp_tx: flume::Sender<H2ResponseChunk>,
}

impl PyH2Bridge {
    pub fn new(
        req_rx: flume::Receiver<Option<Vec<u8>>>,
        resp_tx: flume::Sender<H2ResponseChunk>,
    ) -> Self {
        PyH2Bridge { req_rx, resp_tx }
    }
}

#[pymethods]
impl PyH2Bridge {
    pub fn try_recv_body_chunk(&self) -> PyResult<(bool, Option<Vec<u8>>)> {
        match self.req_rx.try_recv() {
            Ok(chunk) => Ok((true, chunk)),
            Err(flume::TryRecvError::Empty) => Ok((false, None)),
            Err(flume::TryRecvError::Disconnected) => Ok((true, None)),
        }
    }

    pub fn recv_body_chunk(&self) -> PyResult<Option<Vec<u8>>> {
        match self.req_rx.recv() {
            Ok(chunk) => Ok(chunk),
            Err(_) => Ok(None),
        }
    }

    pub fn send_response(
        &self,
        status: u16,
        headers: Vec<(Vec<u8>, Vec<u8>)>,
        body: Vec<u8>,
        more_body: bool,
    ) -> PyResult<bool> {
        Ok(self
            .resp_tx
            .send((status, headers, body, more_body))
            .is_ok())
    }
}

struct PrefixedStream {
    prefix: Option<std::io::Cursor<Vec<u8>>>,
    stream: tokio::net::TcpStream,
}

impl tokio::io::AsyncRead for PrefixedStream {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        if let Some(ref mut prefix) = self.prefix {
            let unfilled = buf.initialize_unfilled();
            if !unfilled.is_empty() {
                match std::io::Read::read(prefix, unfilled) {
                    Ok(n) if n > 0 => {
                        buf.advance(n);
                        if prefix.position() as usize >= prefix.get_ref().len() {
                            self.prefix = None;
                        }
                        return std::task::Poll::Ready(Ok(()));
                    }
                    Ok(_) => {
                        self.prefix = None;
                    }
                    Err(e) => return std::task::Poll::Ready(Err(e)),
                }
            }
        }
        std::pin::Pin::new(&mut self.stream).poll_read(cx, buf)
    }
}

impl tokio::io::AsyncWrite for PrefixedStream {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<Result<usize, std::io::Error>> {
        std::pin::Pin::new(&mut self.stream).poll_write(cx, buf)
    }

    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), std::io::Error>> {
        std::pin::Pin::new(&mut self.stream).poll_flush(cx)
    }

    fn poll_shutdown(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), std::io::Error>> {
        std::pin::Pin::new(&mut self.stream).poll_shutdown(cx)
    }
}

#[pyfunction]
#[allow(clippy::too_many_arguments)]
#[pyo3(signature = (fd, initial_bytes, app, loop_obj, client_host, client_port, server_host, server_port, done_cb=None))]
pub fn serve_h2_connection(
    _py: Python<'_>,
    fd: i32,
    initial_bytes: Vec<u8>,
    app: Py<PyAny>,
    loop_obj: Py<PyAny>,
    client_host: String,
    client_port: u16,
    server_host: String,
    server_port: u16,
    done_cb: Option<Py<PyAny>>,
) -> PyResult<()> {
    use std::os::fd::FromRawFd;

    let std_stream = unsafe { std::net::TcpStream::from_raw_fd(fd) };
    std_stream.set_nonblocking(true)?;
    let rt = get_h2_runtime();
    let app_arc = Arc::new(app);
    let loop_arc = Arc::new(loop_obj);
    let done_cb_arc = done_cb.map(Arc::new);

    rt.spawn(async move {
        let tokio_stream = match tokio::net::TcpStream::from_std(std_stream) {
            Ok(s) => s,
            Err(_) => {
                if let Some(cb) = done_cb_arc {
                    Python::attach(|py| {
                        let loop_ref = loop_arc.bind(py);
                        let cb_ref = cb.bind(py);
                        let _ = loop_ref.call_method1("call_soon_threadsafe", (cb_ref,));
                    });
                }
                return;
            }
        };

        let prefixed = PrefixedStream {
            prefix: if initial_bytes.is_empty() {
                None
            } else {
                Some(std::io::Cursor::new(initial_bytes))
            },
            stream: tokio_stream,
        };

        let mut connection = match h2::server::handshake(prefixed).await {
            Ok(c) => c,
            Err(_) => {
                if let Some(cb) = done_cb_arc {
                    Python::attach(|py| {
                        let loop_ref = loop_arc.bind(py);
                        let cb_ref = cb.bind(py);
                        let _ = loop_ref.call_method1("call_soon_threadsafe", (cb_ref,));
                    });
                }
                return;
            }
        };

        while let Some(result) = connection.accept().await {
            let (request, mut respond) = match result {
                Ok(r) => r,
                Err(_) => break,
            };

            let app_clone = Arc::clone(&app_arc);
            let loop_clone = Arc::clone(&loop_arc);
            let client_h = client_host.clone();
            let server_h = server_host.clone();

            tokio::spawn(async move {
                let method = request.method().as_str().to_string();
                let full_path = request.uri().path().to_string();
                let query = request.uri().query().unwrap_or("").to_string();
                let mut raw_headers = Vec::new();
                for (name, val) in request.headers().iter() {
                    raw_headers.push((
                        name.as_str().to_ascii_lowercase().into_bytes(),
                        val.as_bytes().to_vec(),
                    ));
                }

                let (req_tx, req_rx) = flume::bounded::<Option<Vec<u8>>>(32);
                let (resp_tx, resp_rx) =
                    flume::bounded::<(u16, Vec<(Vec<u8>, Vec<u8>)>, Vec<u8>, bool)>(32);

                // Background request body reader
                let mut body = request.into_body();
                tokio::spawn(async move {
                    while let Some(chunk_res) = body.data().await {
                        if let Ok(bytes) = chunk_res {
                            let _ = body.flow_control().release_capacity(bytes.len());
                            if req_tx.send_async(Some(bytes.to_vec())).await.is_err() {
                                break;
                            }
                        } else {
                            break;
                        }
                    }
                    let _ = req_tx.send_async(None).await;
                });

                // Background response sender
                tokio::spawn(async move {
                    let mut send_body_opt: Option<h2::SendStream<bytes::Bytes>> = None;
                    while let Ok((status, headers, chunk, more_body)) = resp_rx.recv_async().await {
                        if send_body_opt.is_none() {
                            let mut resp = http::Response::builder()
                                .status(
                                    http::StatusCode::from_u16(status)
                                        .unwrap_or(http::StatusCode::OK),
                                )
                                .version(http::Version::HTTP_2);
                            for (k, v) in headers {
                                if let (Ok(name), Ok(val)) = (
                                    http::header::HeaderName::from_bytes(&k),
                                    http::header::HeaderValue::from_bytes(&v),
                                ) {
                                    resp = resp.header(name, val);
                                }
                            }
                            let end_stream = !more_body && chunk.is_empty();
                            let resp_built = match resp.body(()) {
                                Ok(b) => b,
                                Err(_) => break,
                            };
                            match respond.send_response(resp_built, end_stream) {
                                Ok(sb) => {
                                    send_body_opt = Some(sb);
                                    if end_stream {
                                        break;
                                    }
                                }
                                Err(_) => break,
                            }
                        }

                        if let Some(ref mut sb) = send_body_opt {
                            if !chunk.is_empty() || !more_body {
                                let end_stream = !more_body;
                                let b = bytes::Bytes::from(chunk);
                                if sb.send_data(b, end_stream).is_err() {
                                    break;
                                }
                                if end_stream {
                                    break;
                                }
                            }
                        }
                    }
                });

                // Bridge to Python ASGI App
                Python::attach(|py| {
                    let req_rx_py = req_rx.clone();
                    let resp_tx_py = resp_tx.clone();

                    let py_handler = move |py: Python<'_>| -> PyResult<()> {
                        let asyncio_mod = py.import("asyncio")?;

                        // Create scope dict
                        let scope = pyo3::types::PyDict::new(py);
                        scope.set_item("type", "http")?;
                        let asgi_dict = pyo3::types::PyDict::new(py);
                        asgi_dict.set_item("version", "3.0")?;
                        asgi_dict.set_item("spec_version", "2.0")?;
                        scope.set_item("asgi", asgi_dict)?;
                        scope.set_item("http_version", "2")?;
                        scope.set_item("method", method.as_str())?;
                        scope.set_item("path", full_path.as_str())?;
                        scope.set_item(
                            "raw_path",
                            pyo3::types::PyBytes::new(py, full_path.as_bytes()),
                        )?;
                        scope.set_item(
                            "query_string",
                            pyo3::types::PyBytes::new(py, query.as_bytes()),
                        )?;

                        let py_headers = pyo3::types::PyList::empty(py);
                        for (k, v) in &raw_headers {
                            let k_bytes = pyo3::types::PyBytes::new(py, k);
                            let v_bytes = pyo3::types::PyBytes::new(py, v);
                            let pair = pyo3::types::PyTuple::new(
                                py,
                                [k_bytes.as_any(), v_bytes.as_any()],
                            )?;
                            py_headers.append(pair)?;
                        }
                        scope.set_item("headers", py_headers)?;
                        scope.set_item("client", (client_h.as_str(), client_port))?;
                        scope.set_item("server", (server_h.as_str(), server_port))?;

                        let asgi_module = py.import("multiloop.asgi")?;
                        let dispatch_fn = asgi_module.getattr("_dispatch_h2_stream")?;
                        let app_ref = app_clone.bind(py);
                        let loop_ref = loop_clone.bind(py);

                        let bridge = Py::new(py, PyH2Bridge::new(req_rx_py, resp_tx_py))?;
                        let coroutine = dispatch_fn.call1((app_ref, scope, bridge))?;
                        let run_coro = asyncio_mod.getattr("run_coroutine_threadsafe")?;
                        run_coro.call1((coroutine, loop_ref))?;
                        Ok(())
                    };

                    let _ = py_handler(py);
                });
            });
        }

        if let Some(cb) = done_cb_arc {
            Python::attach(|py| {
                let loop_ref = loop_arc.bind(py);
                let cb_ref = cb.bind(py);
                let _ = loop_ref.call_method1("call_soon_threadsafe", (cb_ref,));
            });
        }
    });

    Ok(())
}
