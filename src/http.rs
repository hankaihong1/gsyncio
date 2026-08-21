use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyBool, PyBytes, PyList, PyString, PyTuple};

pub(crate) type ParsedHttpRequest<'py> = (
    Bound<'py, PyString>,
    Bound<'py, PyString>,
    Bound<'py, PyBytes>,
    Bound<'py, PyBytes>,
    Bound<'py, PyString>,
    Bound<'py, PyList>,
    usize,
    isize,
    bool,
    bool,
    bool,
    Bound<'py, PyBytes>,
);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HttpParserState {
    WaitingHeader,
    ReceivingContentLength { remaining: usize },
    ReceivingChunked { stage: ChunkedStage },
    ServingApp,
    KeepAliveWait,
    Closed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChunkedStage {
    ReadingSize,
    ReadingData { chunk_len: usize },
    ReadingTrailers,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectionMode {
    Http11,
    WebSocket,
}

/// High-performance stateful HTTP/1.1 and WebSocket protocol engine.
///
/// Acts as a pure protocol state machine and wire serializer. Holds internal
/// byte buffers and manages request parsing, RFC 9112 chunked transfer decoding,
/// request smuggling prevention, Keep-Alive pipelining, and RFC 6455 framing.
#[pyclass(module = "multiloop._multiloop_core")]
pub struct FastHttpConnection {
    buffer: Vec<u8>,
    cursor: usize,
    state: HttpParserState,
    mode: ConnectionMode,
    max_body_size: usize,
    max_header_size: usize,
    request_count: usize,
    max_keepalive_requests: usize,
    keep_alive: bool,
    ws_fragment_buffer: Vec<u8>,
    ws_fragment_opcode: u8,
    total_body_received: usize,
}

#[pymethods]
impl FastHttpConnection {
    #[new]
    #[pyo3(signature = (max_body_size = 10 * 1024 * 1024, max_header_size = 64 * 1024, max_keepalive_requests = 1000))]
    pub fn new(
        max_body_size: usize,
        max_header_size: usize,
        max_keepalive_requests: usize,
    ) -> Self {
        Self {
            buffer: Vec::with_capacity(8192),
            cursor: 0,
            state: HttpParserState::WaitingHeader,
            mode: ConnectionMode::Http11,
            max_body_size,
            max_header_size,
            request_count: 0,
            max_keepalive_requests,
            keep_alive: true,
            ws_fragment_buffer: Vec::new(),
            ws_fragment_opcode: 0,
            total_body_received: 0,
        }
    }

    #[pyo3(signature = (data = None))]
    pub fn feed_data<'py>(
        &mut self,
        py: Python<'py>,
        data: Option<&Bound<'py, PyAny>>,
    ) -> PyResult<Bound<'py, PyList>> {
        if let Some(d) = data {
            let py_buf = pyo3::buffer::PyBuffer::<u8>::get(d)?;
            let slice = unsafe {
                std::slice::from_raw_parts(py_buf.buf_ptr() as *const u8, py_buf.len_bytes())
            };
            if !slice.is_empty() {
                self.buffer.extend_from_slice(slice);
            }
        }
        self.process_events(py)
    }

    pub fn pump_events<'py>(&mut self, py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
        self.process_events(py)
    }

    pub fn reset_for_next_request(&mut self) {
        if self.state == HttpParserState::ServingApp {
            self.total_body_received = 0;
            if !self.keep_alive || self.request_count >= self.max_keepalive_requests {
                self.state = HttpParserState::Closed;
            } else {
                self.state = HttpParserState::KeepAliveWait;
            }
        }
    }

    pub fn switch_to_websocket(&mut self) {
        self.mode = ConnectionMode::WebSocket;
    }

    pub fn is_closed(&self) -> bool {
        self.state == HttpParserState::Closed
    }

    pub fn close(&mut self) {
        self.state = HttpParserState::Closed;
    }

    /// Fast zero-alloc HTTP/1.1 response wire serializer and CRLF safety scanner.
    /// Returns: (response_bytes, is_chunked, keep_alive)
    #[staticmethod]
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (status_code, server_header, date_header, headers, body, more_body, is_no_body_status, keep_alive))]
    pub fn format_response<'py>(
        py: Python<'py>,
        status_code: u16,
        server_header: &[u8],
        date_header: &[u8],
        headers: &Bound<'py, PyAny>,
        body: &[u8],
        more_body: bool,
        is_no_body_status: bool,
        mut keep_alive: bool,
    ) -> PyResult<(Bound<'py, PyBytes>, bool, bool)> {
        let mut has_cl = false;
        let mut has_conn = false;
        let mut has_server = false;
        let mut has_date = false;
        let mut parsed_headers: Vec<(Bound<'py, PyBytes>, Bound<'py, PyBytes>)> = Vec::new();

        let iterator = headers.try_iter()?;
        for item_res in iterator {
            let item = item_res?;
            let name_any = item.get_item(0)?;
            let val_any = item.get_item(1)?;
            let name_py: Bound<'py, PyBytes> = name_any.extract()?;
            let val_py: Bound<'py, PyBytes> = val_any.extract()?;

            let name_bytes = name_py.as_bytes();
            let val_bytes = val_py.as_bytes();

            if name_bytes.contains(&b'\r')
                || name_bytes.contains(&b'\n')
                || val_bytes.contains(&b'\r')
                || val_bytes.contains(&b'\n')
            {
                return Err(PyValueError::new_err(
                    "Illegal CRLF control characters detected in HTTP response header",
                ));
            }

            if name_bytes.eq_ignore_ascii_case(b"content-length") {
                has_cl = true;
            } else if name_bytes.eq_ignore_ascii_case(b"connection") {
                has_conn = true;
                let v_lower = val_bytes.to_ascii_lowercase();
                if v_lower.windows(5).any(|w| w == b"close")
                    || val_bytes.eq_ignore_ascii_case(b"close")
                {
                    keep_alive = false;
                }
            } else if name_bytes.eq_ignore_ascii_case(b"server") {
                has_server = true;
            } else if name_bytes.eq_ignore_ascii_case(b"date") {
                has_date = true;
            }
            parsed_headers.push((name_py, val_py));
        }

        let reason = match status_code {
            200 => "OK",
            201 => "Created",
            204 => "No Content",
            301 => "Moved Permanently",
            302 => "Found",
            304 => "Not Modified",
            400 => "Bad Request",
            401 => "Unauthorized",
            403 => "Forbidden",
            404 => "Not Found",
            405 => "Method Not Allowed",
            413 => "Payload Too Large",
            426 => "Upgrade Required",
            431 => "Request Header Fields Too Large",
            500 => "Internal Server Error",
            502 => "Bad Gateway",
            503 => "Service Unavailable",
            _ => "OK",
        };
        let status_line = format!("HTTP/1.1 {} {}\r\n", status_code, reason);
        let status_bytes = status_line.as_bytes();

        let mut is_resp_chunked = false;
        let mut injected_cl = Vec::new();
        let mut injected_te: &[u8] = b"";

        if !is_no_body_status {
            if !more_body && !has_cl {
                injected_cl.extend_from_slice(b"content-length: ");
                injected_cl.extend_from_slice(body.len().to_string().as_bytes());
                injected_cl.extend_from_slice(b"\r\n");
            } else if more_body && !has_cl {
                injected_te = b"transfer-encoding: chunked\r\n";
                is_resp_chunked = true;
            }
        }

        let injected_conn: &[u8] = if !has_conn {
            if keep_alive {
                b"connection: keep-alive\r\n"
            } else {
                b"connection: close\r\n"
            }
        } else {
            b""
        };

        let s_hdr = if has_server { b"" } else { server_header };
        let d_hdr = if has_date { b"" } else { date_header };

        let mut total_len = status_bytes.len()
            + s_hdr.len()
            + d_hdr.len()
            + injected_cl.len()
            + injected_te.len()
            + injected_conn.len()
            + 2;

        for (name, val) in &parsed_headers {
            total_len += name.as_bytes().len() + 2 + val.as_bytes().len() + 2;
        }

        let mut chunk_prefix = Vec::new();
        let mut chunk_suffix: &[u8] = b"";

        if !is_no_body_status {
            if is_resp_chunked {
                if !body.is_empty() {
                    let hex_len = format!("{:X}\r\n", body.len());
                    chunk_prefix.extend_from_slice(hex_len.as_bytes());
                    chunk_suffix = b"\r\n";
                }
                total_len += chunk_prefix.len() + body.len() + chunk_suffix.len();
                if !more_body {
                    total_len += 5; // "0\r\n\r\n"
                }
            } else {
                total_len += body.len();
            }
        }

        let py_bytes = PyBytes::new_with(py, total_len, |out| {
            let mut cursor = 0;
            let mut append = |src: &[u8]| {
                out[cursor..cursor + src.len()].copy_from_slice(src);
                cursor += src.len();
            };

            append(status_bytes);
            if !s_hdr.is_empty() {
                append(s_hdr);
            }
            if !d_hdr.is_empty() {
                append(d_hdr);
            }

            for (name, val) in &parsed_headers {
                append(name.as_bytes());
                append(b": ");
                append(val.as_bytes());
                append(b"\r\n");
            }

            if !injected_cl.is_empty() {
                append(&injected_cl);
            }
            if !injected_te.is_empty() {
                append(injected_te);
            }
            if !injected_conn.is_empty() {
                append(injected_conn);
            }
            append(b"\r\n");

            if !is_no_body_status {
                if is_resp_chunked {
                    if !body.is_empty() {
                        append(&chunk_prefix);
                        append(body);
                        append(chunk_suffix);
                    }
                    if !more_body {
                        append(b"0\r\n\r\n");
                    }
                } else if !body.is_empty() {
                    append(body);
                }
            }
            Ok(())
        })?;

        Ok((py_bytes, is_resp_chunked, keep_alive))
    }

    #[staticmethod]
    pub fn format_chunk<'py>(
        py: Python<'py>,
        chunk: &[u8],
        more_body: bool,
    ) -> PyResult<Bound<'py, PyBytes>> {
        let mut total_len = 0;
        let mut prefix = Vec::new();
        let mut suffix: &[u8] = b"";
        if !chunk.is_empty() {
            let hex_len = format!("{:X}\r\n", chunk.len());
            prefix.extend_from_slice(hex_len.as_bytes());
            suffix = b"\r\n";
            total_len += prefix.len() + chunk.len() + suffix.len();
        }
        if !more_body {
            total_len += 5; // "0\r\n\r\n"
        }
        PyBytes::new_with(py, total_len, |out| {
            let mut cursor = 0;
            let mut append = |src: &[u8]| {
                out[cursor..cursor + src.len()].copy_from_slice(src);
                cursor += src.len();
            };
            if !chunk.is_empty() {
                append(&prefix);
                append(chunk);
                append(suffix);
            }
            if !more_body {
                append(b"0\r\n\r\n");
            }
            Ok(())
        })
    }

    #[staticmethod]
    pub fn format_ws_frame<'py>(
        py: Python<'py>,
        opcode: u8,
        payload: &[u8],
    ) -> PyResult<Bound<'py, PyBytes>> {
        let mut header = Vec::with_capacity(10);
        header.push(0x80 | (opcode & 0x0F));
        let len = payload.len();
        if len < 126 {
            header.push(len as u8);
        } else if len <= 0xFFFF {
            header.push(126);
            header.extend_from_slice(&(len as u16).to_be_bytes());
        } else {
            header.push(127);
            header.extend_from_slice(&(len as u64).to_be_bytes());
        }
        let total_len = header.len() + len;
        PyBytes::new_with(py, total_len, |out| {
            out[..header.len()].copy_from_slice(&header);
            out[header.len()..].copy_from_slice(payload);
            Ok(())
        })
    }
}

impl FastHttpConnection {
    fn compact_buffer(&mut self) {
        if self.cursor == self.buffer.len() {
            self.buffer.clear();
            self.cursor = 0;
        } else if self.cursor >= 64 * 1024 && self.cursor >= self.buffer.len() / 2 {
            self.buffer.drain(0..self.cursor);
            self.cursor = 0;
        }
    }

    fn emit_error<'py>(
        &mut self,
        py: Python<'py>,
        events: &Bound<'py, PyList>,
        status: u16,
        msg: &[u8],
    ) -> PyResult<()> {
        self.state = HttpParserState::Closed;
        let resp = format!(
            "HTTP/1.1 {} {}\r\nserver: multiloop\r\nconnection: close\r\ncontent-length: {}\r\n\r\n",
            status,
            match status {
                400 => "Bad Request",
                413 => "Payload Too Large",
                431 => "Request Header Fields Too Large",
                _ => "Internal Server Error",
            },
            msg.len()
        );
        let mut err_bytes = Vec::from(resp.as_bytes());
        err_bytes.extend_from_slice(msg);
        let py_err = PyBytes::new(py, &err_bytes);
        let ev = PyTuple::new(
            py,
            [
                (5u32).into_pyobject(py)?.to_owned().into_any(),
                (status as u32).into_pyobject(py)?.to_owned().into_any(),
                py_err.into_any(),
            ],
        )?;
        events.append(ev)
    }

    fn process_events<'py>(&mut self, py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
        let events = PyList::empty(py);
        self.compact_buffer();

        if self.mode == ConnectionMode::WebSocket {
            return self.process_websocket_events(py, &events);
        }

        loop {
            if self.state == HttpParserState::Closed {
                break;
            }

            if self.state == HttpParserState::ServingApp {
                break;
            }

            if self.state == HttpParserState::WaitingHeader
                || self.state == HttpParserState::KeepAliveWait
            {
                let unread = &self.buffer[self.cursor..];
                if unread.is_empty() {
                    break;
                }

                if unread.len() > self.max_header_size {
                    self.emit_error(py, &events, 431, b"Request Header Fields Too Large")?;
                    break;
                }

                let mut stack_headers = [httparse::EMPTY_HEADER; 256];
                let mut req = httparse::Request::new(&mut stack_headers);
                let parse_res = req.parse(unread);

                let (req_method, req_path, req_version, headers_slice, body_offset) =
                    match parse_res {
                        Ok(httparse::Status::Complete(offset)) => {
                            (req.method, req.path, req.version, req.headers, offset)
                        }
                        Ok(httparse::Status::Partial) => break,
                        Err(_) => {
                            self.emit_error(py, &events, 400, b"Bad Request")?;
                            break;
                        }
                    };

                self.cursor += body_offset;
                self.request_count += 1;

                let method_str = PyString::new(py, req_method.unwrap_or("GET"));
                let full_path = req_path.unwrap_or("/");
                let (path_str, raw_path_bytes, query_bytes) = match full_path.split_once('?') {
                    Some((p, q)) => (
                        PyString::new(py, p),
                        PyBytes::new(py, p.as_bytes()),
                        PyBytes::new(py, q.as_bytes()),
                    ),
                    None => (
                        PyString::new(py, full_path),
                        PyBytes::new(py, full_path.as_bytes()),
                        PyBytes::new(py, b""),
                    ),
                };

                let version_str = match req_version.unwrap_or(1) {
                    0 => PyString::new(py, "1.0"),
                    1 => PyString::new(py, "1.1"),
                    v => PyString::new(py, &format!("1.{}", v)),
                };

                let mut header_tuples = Vec::with_capacity(headers_slice.len());
                let mut content_length: isize = -1;
                let mut has_cl = false;
                let mut is_chunked = false;
                let mut te_count = 0;
                let mut cl_conflict = false;
                let mut keep_alive = req_version.unwrap_or(1) == 1;
                let mut is_upgrade = false;
                let mut upgrade_proto = PyBytes::new(py, b"");

                for h in headers_slice.iter() {
                    if !h.name.is_empty() {
                        let name_lower: Vec<u8> =
                            h.name.bytes().map(|b| b.to_ascii_lowercase()).collect();
                        let py_name = PyBytes::new(py, &name_lower);
                        let py_val = PyBytes::new(py, h.value);

                        if name_lower == b"content-length" {
                            if let Ok(s) = std::str::from_utf8(h.value) {
                                if let Ok(val) = s.trim().parse::<usize>() {
                                    if has_cl && (val as isize) != content_length {
                                        cl_conflict = true;
                                    } else {
                                        content_length = val as isize;
                                        has_cl = true;
                                    }
                                } else {
                                    cl_conflict = true;
                                }
                            } else {
                                cl_conflict = true;
                            }
                        } else if name_lower == b"transfer-encoding" {
                            te_count += 1;
                            if let Ok(val_str) = std::str::from_utf8(h.value) {
                                let codings: Vec<&str> = val_str
                                    .split(',')
                                    .map(|s| s.trim())
                                    .filter(|s| !s.is_empty())
                                    .collect();
                                if let Some(&last_coding) = codings.last() {
                                    if last_coding.eq_ignore_ascii_case("chunked") {
                                        is_chunked = true;
                                    } else {
                                        cl_conflict = true;
                                    }
                                } else {
                                    cl_conflict = true;
                                }
                            } else {
                                cl_conflict = true;
                            }
                        } else if name_lower == b"connection" {
                            let v_lower = h.value.to_ascii_lowercase();
                            if v_lower.windows(5).any(|w| w == b"close")
                                || h.value.eq_ignore_ascii_case(b"close")
                            {
                                keep_alive = false;
                            } else if v_lower.windows(10).any(|w| w == b"keep-alive")
                                || h.value.eq_ignore_ascii_case(b"keep-alive")
                            {
                                keep_alive = true;
                            }
                            if v_lower.windows(7).any(|w| w == b"upgrade")
                                || h.value.eq_ignore_ascii_case(b"upgrade")
                            {
                                is_upgrade = true;
                            }
                        } else if name_lower == b"upgrade" {
                            upgrade_proto = PyBytes::new(py, &h.value.to_ascii_lowercase());
                        }

                        let tuple = PyTuple::new(py, [py_name, py_val])?;
                        header_tuples.push(tuple);
                    }
                }

                if cl_conflict || (is_chunked && has_cl) || (te_count > 1 && !is_chunked) {
                    self.emit_error(py, &events, 400, b"Bad Request: Conflicting HTTP headers")?;
                    break;
                }

                if is_upgrade && upgrade_proto.as_bytes() == b"websocket" {
                    self.keep_alive = keep_alive;
                    let py_headers = PyList::new(py, header_tuples)?;
                    let req_start_event = PyTuple::new(
                        py,
                        [
                            (1u32).into_pyobject(py)?.to_owned().into_any(),
                            method_str.into_any(),
                            path_str.into_any(),
                            raw_path_bytes.into_any(),
                            query_bytes.into_any(),
                            version_str.into_any(),
                            py_headers.into_any(),
                            PyBool::new(py, keep_alive).as_any().clone(),
                            PyBool::new(py, is_upgrade).as_any().clone(),
                            upgrade_proto.clone().into_any(),
                            PyBool::new(py, false).as_any().clone(),
                        ],
                    )?;
                    events.append(req_start_event)?;
                    self.state = HttpParserState::ServingApp;
                    let upg_ev = PyTuple::new(
                        py,
                        [
                            (3u32).into_pyobject(py)?.to_owned().into_any(),
                            upgrade_proto.into_any(),
                        ],
                    )?;
                    events.append(upg_ev)?;
                    break;
                }

                if is_chunked {
                    self.keep_alive = keep_alive;
                    let py_headers = PyList::new(py, header_tuples)?;
                    let req_start_event = PyTuple::new(
                        py,
                        [
                            (1u32).into_pyobject(py)?.to_owned().into_any(),
                            method_str.into_any(),
                            path_str.into_any(),
                            raw_path_bytes.into_any(),
                            query_bytes.into_any(),
                            version_str.into_any(),
                            py_headers.into_any(),
                            PyBool::new(py, keep_alive).as_any().clone(),
                            PyBool::new(py, is_upgrade).as_any().clone(),
                            upgrade_proto.clone().into_any(),
                            PyBool::new(py, true).as_any().clone(),
                        ],
                    )?;
                    events.append(req_start_event)?;
                    self.state = HttpParserState::ReceivingChunked {
                        stage: ChunkedStage::ReadingSize,
                    };
                } else if content_length > 0 {
                    let cl = content_length as usize;
                    if cl > self.max_body_size {
                        self.emit_error(py, &events, 413, b"Payload Too Large")?;
                        break;
                    }
                    self.keep_alive = keep_alive;
                    let py_headers = PyList::new(py, header_tuples)?;
                    let req_start_event = PyTuple::new(
                        py,
                        [
                            (1u32).into_pyobject(py)?.to_owned().into_any(),
                            method_str.into_any(),
                            path_str.into_any(),
                            raw_path_bytes.into_any(),
                            query_bytes.into_any(),
                            version_str.into_any(),
                            py_headers.into_any(),
                            PyBool::new(py, keep_alive).as_any().clone(),
                            PyBool::new(py, is_upgrade).as_any().clone(),
                            upgrade_proto.clone().into_any(),
                            PyBool::new(py, true).as_any().clone(),
                        ],
                    )?;
                    events.append(req_start_event)?;

                    let available = self.buffer.len() - self.cursor;
                    if available >= cl {
                        let body_bytes =
                            PyBytes::new(py, &self.buffer[self.cursor..self.cursor + cl]);
                        self.cursor += cl;
                        self.state = HttpParserState::ServingApp;
                        let chunk_ev = PyTuple::new(
                            py,
                            [
                                (2u32).into_pyobject(py)?.to_owned().into_any(),
                                body_bytes.into_any(),
                                PyBool::new(py, false).as_any().clone(),
                            ],
                        )?;
                        events.append(chunk_ev)?;
                    } else {
                        let remaining = cl - available;
                        self.state = HttpParserState::ReceivingContentLength { remaining };
                        if available > 0 {
                            let body_bytes = PyBytes::new(
                                py,
                                &self.buffer[self.cursor..self.cursor + available],
                            );
                            self.cursor += available;
                            let chunk_ev = PyTuple::new(
                                py,
                                [
                                    (2u32).into_pyobject(py)?.to_owned().into_any(),
                                    body_bytes.into_any(),
                                    PyBool::new(py, true).as_any().clone(),
                                ],
                            )?;
                            events.append(chunk_ev)?;
                        }
                    }
                } else {
                    self.keep_alive = keep_alive;
                    let py_headers = PyList::new(py, header_tuples)?;
                    let req_start_event = PyTuple::new(
                        py,
                        [
                            (1u32).into_pyobject(py)?.to_owned().into_any(),
                            method_str.into_any(),
                            path_str.into_any(),
                            raw_path_bytes.into_any(),
                            query_bytes.into_any(),
                            version_str.into_any(),
                            py_headers.into_any(),
                            PyBool::new(py, keep_alive).as_any().clone(),
                            PyBool::new(py, is_upgrade).as_any().clone(),
                            upgrade_proto.clone().into_any(),
                            PyBool::new(py, false).as_any().clone(),
                        ],
                    )?;
                    events.append(req_start_event)?;
                    self.state = HttpParserState::ServingApp;
                }
            } else if let HttpParserState::ReceivingContentLength { remaining } = self.state {
                let unread_len = self.buffer.len() - self.cursor;
                if unread_len == 0 {
                    break;
                }
                let consume = std::cmp::min(unread_len, remaining);
                let chunk_bytes =
                    PyBytes::new(py, &self.buffer[self.cursor..self.cursor + consume]);
                self.cursor += consume;
                let new_remaining = remaining - consume;
                let more_body = new_remaining > 0;
                if more_body {
                    self.state = HttpParserState::ReceivingContentLength {
                        remaining: new_remaining,
                    };
                } else {
                    self.state = HttpParserState::ServingApp;
                }
                let chunk_event = PyTuple::new(
                    py,
                    [
                        (2u32).into_pyobject(py)?.to_owned().into_any(),
                        chunk_bytes.into_any(),
                        PyBool::new(py, more_body).as_any().clone(),
                    ],
                )?;
                events.append(chunk_event)?;
            } else if let HttpParserState::ReceivingChunked { stage } = self.state {
                match stage {
                    ChunkedStage::ReadingSize => {
                        let unread = &self.buffer[self.cursor..];
                        if let Some(pos) = unread.windows(2).position(|w| w == b"\r\n") {
                            let size_line = &unread[..pos];
                            let size_str = size_line.split(|&b| b == b';').next().unwrap_or(b"");
                            let size_trim = match std::str::from_utf8(size_str) {
                                Ok(s) => s.trim(),
                                Err(_) => {
                                    self.emit_error(py, &events, 400, b"Invalid chunked encoding")?;
                                    break;
                                }
                            };
                            let chunk_len = match usize::from_str_radix(size_trim, 16) {
                                Ok(l) => l,
                                Err(_) => {
                                    self.emit_error(py, &events, 400, b"Invalid chunk length")?;
                                    break;
                                }
                            };

                            self.cursor += pos + 2;
                            if chunk_len == 0 {
                                self.state = HttpParserState::ReceivingChunked {
                                    stage: ChunkedStage::ReadingTrailers,
                                };
                            } else {
                                self.total_body_received += chunk_len;
                                if self.total_body_received > self.max_body_size {
                                    self.emit_error(py, &events, 413, b"Payload Too Large")?;
                                    break;
                                }
                                self.state = HttpParserState::ReceivingChunked {
                                    stage: ChunkedStage::ReadingData { chunk_len },
                                };
                            }
                        } else {
                            break;
                        }
                    }
                    ChunkedStage::ReadingData { chunk_len } => {
                        if self.buffer.len() - self.cursor < chunk_len + 2 {
                            break;
                        }
                        if &self.buffer[self.cursor + chunk_len..self.cursor + chunk_len + 2]
                            != b"\r\n"
                        {
                            self.emit_error(py, &events, 400, b"Malformed chunk boundary")?;
                            break;
                        }
                        let chunk_bytes =
                            PyBytes::new(py, &self.buffer[self.cursor..self.cursor + chunk_len]);
                        self.cursor += chunk_len + 2;
                        self.state = HttpParserState::ReceivingChunked {
                            stage: ChunkedStage::ReadingSize,
                        };
                        let chunk_ev = PyTuple::new(
                            py,
                            [
                                (2u32).into_pyobject(py)?.to_owned().into_any(),
                                chunk_bytes.into_any(),
                                PyBool::new(py, true).as_any().clone(),
                            ],
                        )?;
                        events.append(chunk_ev)?;
                    }
                    ChunkedStage::ReadingTrailers => {
                        let unread = &self.buffer[self.cursor..];
                        if unread.starts_with(b"\r\n") {
                            self.cursor += 2;
                            self.state = HttpParserState::ServingApp;
                            let end_ev = PyTuple::new(
                                py,
                                [
                                    (2u32).into_pyobject(py)?.to_owned().into_any(),
                                    PyBytes::new(py, b"").into_any(),
                                    PyBool::new(py, false).as_any().clone(),
                                ],
                            )?;
                            events.append(end_ev)?;
                        } else if let Some(pos) = unread.windows(4).position(|w| w == b"\r\n\r\n") {
                            self.cursor += pos + 4;
                            self.state = HttpParserState::ServingApp;
                            let end_ev = PyTuple::new(
                                py,
                                [
                                    (2u32).into_pyobject(py)?.to_owned().into_any(),
                                    PyBytes::new(py, b"").into_any(),
                                    PyBool::new(py, false).as_any().clone(),
                                ],
                            )?;
                            events.append(end_ev)?;
                        } else {
                            break;
                        }
                    }
                }
            }
        }

        Ok(events)
    }

    fn process_websocket_events<'py>(
        &mut self,
        py: Python<'py>,
        events: &Bound<'py, PyList>,
    ) -> PyResult<Bound<'py, PyList>> {
        while self.buffer.len() - self.cursor >= 2 {
            let b1 = self.buffer[self.cursor];
            let b2 = self.buffer[self.cursor + 1];
            let fin = (b1 & 0x80) != 0;
            let opcode = b1 & 0x0F;
            let masked = (b2 & 0x80) != 0;
            let raw_len = (b2 & 0x7F) as usize;

            if !masked {
                self.state = HttpParserState::Closed;
                let close_frame =
                    FastHttpConnection::format_ws_frame(py, 0x8, &(1002u16).to_be_bytes())?;
                let ev = PyTuple::new(
                    py,
                    [
                        (5u32).into_pyobject(py)?.to_owned().into_any(),
                        (1002u32).into_pyobject(py)?.to_owned().into_any(),
                        close_frame.into_any(),
                    ],
                )?;
                events.append(ev)?;
                break;
            }

            let (payload_len, header_len) = match raw_len {
                126 => {
                    if self.buffer.len() - self.cursor < 4 {
                        break;
                    }
                    let len = u16::from_be_bytes([
                        self.buffer[self.cursor + 2],
                        self.buffer[self.cursor + 3],
                    ]) as usize;
                    (len, 4)
                }
                127 => {
                    if self.buffer.len() - self.cursor < 10 {
                        break;
                    }
                    let len = u64::from_be_bytes(
                        self.buffer[self.cursor + 2..self.cursor + 10]
                            .try_into()
                            .unwrap(),
                    ) as usize;
                    (len, 10)
                }
                len => (len, 2),
            };

            let total_header_len = header_len + 4;
            if self.buffer.len() - self.cursor < total_header_len + payload_len {
                break;
            }

            let mask = [
                self.buffer[self.cursor + header_len],
                self.buffer[self.cursor + header_len + 1],
                self.buffer[self.cursor + header_len + 2],
                self.buffer[self.cursor + header_len + 3],
            ];

            let payload_offset = self.cursor + total_header_len;
            let slice = &self.buffer[payload_offset..payload_offset + payload_len];
            let unmasked_py_bytes = crate::websocket::fast_websocket_unmask_raw(py, slice, mask)?;
            self.cursor += total_header_len + payload_len;

            if opcode >= 0x8 {
                if payload_len > 125 || !fin {
                    self.state = HttpParserState::Closed;
                    let close_frame =
                        FastHttpConnection::format_ws_frame(py, 0x8, &(1002u16).to_be_bytes())?;
                    let ev = PyTuple::new(
                        py,
                        [
                            (5u32).into_pyobject(py)?.to_owned().into_any(),
                            (1002u32).into_pyobject(py)?.to_owned().into_any(),
                            close_frame.into_any(),
                        ],
                    )?;
                    events.append(ev)?;
                    break;
                }
                let ws_event = PyTuple::new(
                    py,
                    [
                        (4u32).into_pyobject(py)?.to_owned().into_any(),
                        (opcode as u32).into_pyobject(py)?.to_owned().into_any(),
                        PyBool::new(py, fin).as_any().clone(),
                        unmasked_py_bytes.into_any(),
                    ],
                )?;
                events.append(ws_event)?;
            } else if opcode == 0x1 || opcode == 0x2 {
                if self.ws_fragment_opcode != 0 {
                    self.state = HttpParserState::Closed;
                    let close_frame =
                        FastHttpConnection::format_ws_frame(py, 0x8, &(1002u16).to_be_bytes())?;
                    let ev = PyTuple::new(
                        py,
                        [
                            (5u32).into_pyobject(py)?.to_owned().into_any(),
                            (1002u32).into_pyobject(py)?.to_owned().into_any(),
                            close_frame.into_any(),
                        ],
                    )?;
                    events.append(ev)?;
                    break;
                }
                if fin {
                    if opcode == 0x1 && std::str::from_utf8(unmasked_py_bytes.as_bytes()).is_err() {
                        self.state = HttpParserState::Closed;
                        let close_frame =
                            FastHttpConnection::format_ws_frame(py, 0x8, &(1007u16).to_be_bytes())?;
                        let ev = PyTuple::new(
                            py,
                            [
                                (5u32).into_pyobject(py)?.to_owned().into_any(),
                                (1007u32).into_pyobject(py)?.to_owned().into_any(),
                                close_frame.into_any(),
                            ],
                        )?;
                        events.append(ev)?;
                        break;
                    }
                    let ws_event = PyTuple::new(
                        py,
                        [
                            (4u32).into_pyobject(py)?.to_owned().into_any(),
                            (opcode as u32).into_pyobject(py)?.to_owned().into_any(),
                            PyBool::new(py, fin).as_any().clone(),
                            unmasked_py_bytes.into_any(),
                        ],
                    )?;
                    events.append(ws_event)?;
                } else {
                    self.ws_fragment_opcode = opcode;
                    self.ws_fragment_buffer = unmasked_py_bytes.as_bytes().to_vec();
                }
            } else if opcode == 0x0 {
                if self.ws_fragment_opcode == 0 {
                    self.state = HttpParserState::Closed;
                    let close_frame =
                        FastHttpConnection::format_ws_frame(py, 0x8, &(1002u16).to_be_bytes())?;
                    let ev = PyTuple::new(
                        py,
                        [
                            (5u32).into_pyobject(py)?.to_owned().into_any(),
                            (1002u32).into_pyobject(py)?.to_owned().into_any(),
                            close_frame.into_any(),
                        ],
                    )?;
                    events.append(ev)?;
                    break;
                }
                self.ws_fragment_buffer
                    .extend_from_slice(unmasked_py_bytes.as_bytes());
                if fin {
                    let op = self.ws_fragment_opcode;
                    self.ws_fragment_opcode = 0;
                    let full_payload = std::mem::take(&mut self.ws_fragment_buffer);
                    if op == 0x1 && std::str::from_utf8(&full_payload).is_err() {
                        self.state = HttpParserState::Closed;
                        let close_frame =
                            FastHttpConnection::format_ws_frame(py, 0x8, &(1007u16).to_be_bytes())?;
                        let ev = PyTuple::new(
                            py,
                            [
                                (5u32).into_pyobject(py)?.to_owned().into_any(),
                                (1007u32).into_pyobject(py)?.to_owned().into_any(),
                                close_frame.into_any(),
                            ],
                        )?;
                        events.append(ev)?;
                        break;
                    }
                    let py_full = PyBytes::new(py, &full_payload);
                    let ws_event = PyTuple::new(
                        py,
                        [
                            (4u32).into_pyobject(py)?.to_owned().into_any(),
                            (op as u32).into_pyobject(py)?.to_owned().into_any(),
                            PyBool::new(py, fin).as_any().clone(),
                            py_full.into_any(),
                        ],
                    )?;
                    events.append(ws_event)?;
                }
            }
        }
        Ok(events.clone())
    }
}

/// Zero-copy HTTP/1.x request header parser powered by SIMD httparse.
#[pyclass(module = "multiloop._multiloop_core")]
pub struct FastHttpParser;

#[pymethods]
impl FastHttpParser {
    #[staticmethod]
    pub fn parse_request<'py>(
        py: Python<'py>,
        buf_obj: &Bound<'py, PyAny>,
    ) -> PyResult<Option<ParsedHttpRequest<'py>>> {
        let py_buf = pyo3::buffer::PyBuffer::<u8>::get(buf_obj)?;
        let buf = unsafe {
            std::slice::from_raw_parts(py_buf.buf_ptr() as *const u8, py_buf.len_bytes())
        };
        let mut stack_headers = [httparse::EMPTY_HEADER; 128];
        let mut req = httparse::Request::new(&mut stack_headers);
        let parse_res = req.parse(buf);

        let (req_method, req_path, req_version, headers_slice, body_offset) = match parse_res {
            Ok(httparse::Status::Complete(body_offset)) => {
                (req.method, req.path, req.version, req.headers, body_offset)
            }
            Ok(httparse::Status::Partial) => return Ok(None),
            Err(httparse::Error::TooManyHeaders) => {
                let mut heap_headers = vec![httparse::EMPTY_HEADER; 512];
                let mut heap_req = httparse::Request::new(&mut heap_headers);
                match heap_req.parse(buf) {
                    Ok(httparse::Status::Complete(body_offset)) => {
                        return Self::build_parsed_request(
                            py,
                            heap_req.method,
                            heap_req.path,
                            heap_req.version,
                            heap_req.headers,
                            body_offset,
                        );
                    }
                    Ok(httparse::Status::Partial) => return Ok(None),
                    Err(e) => {
                        return Err(PyRuntimeError::new_err(format!(
                            "Malformed HTTP request: {}",
                            e
                        )));
                    }
                }
            }
            Err(e) => {
                return Err(PyRuntimeError::new_err(format!(
                    "Malformed HTTP request: {}",
                    e
                )));
            }
        };

        Self::build_parsed_request(
            py,
            req_method,
            req_path,
            req_version,
            headers_slice,
            body_offset,
        )
    }

    #[staticmethod]
    #[allow(clippy::too_many_arguments)]
    pub fn format_response<'py>(
        py: Python<'py>,
        status_line: &[u8],
        server_header: &[u8],
        date_header: &[u8],
        headers: &Bound<'py, PyAny>,
        body: &[u8],
        more_body: bool,
        is_no_body_status: bool,
        keep_alive: bool,
    ) -> PyResult<(Bound<'py, PyBytes>, bool, bool)> {
        let status_code = if status_line.len() >= 12 && status_line.starts_with(b"HTTP/1.1 ") {
            std::str::from_utf8(&status_line[9..12])
                .ok()
                .and_then(|s| s.parse::<u16>().ok())
                .unwrap_or(200)
        } else {
            200
        };

        FastHttpConnection::format_response(
            py,
            status_code,
            server_header,
            date_header,
            headers,
            body,
            more_body,
            is_no_body_status,
            keep_alive,
        )
    }
}

impl FastHttpParser {
    fn build_parsed_request<'py>(
        py: Python<'py>,
        method_opt: Option<&str>,
        path_opt: Option<&str>,
        version_opt: Option<u8>,
        headers: &[httparse::Header<'_>],
        body_offset: usize,
    ) -> PyResult<Option<ParsedHttpRequest<'py>>> {
        let method = PyString::new(py, method_opt.unwrap_or("GET"));
        let full_path = path_opt.unwrap_or("/");
        let (path_str, raw_path_bytes, query_bytes) = match full_path.split_once('?') {
            Some((p, q)) => (
                PyString::new(py, p),
                PyBytes::new(py, p.as_bytes()),
                PyBytes::new(py, q.as_bytes()),
            ),
            None => (
                PyString::new(py, full_path),
                PyBytes::new(py, full_path.as_bytes()),
                PyBytes::new(py, b""),
            ),
        };

        let version = match version_opt.unwrap_or(1) {
            0 => PyString::new(py, "1.0"),
            1 => PyString::new(py, "1.1"),
            v => PyString::new(py, &format!("1.{}", v)),
        };

        let mut header_tuples = Vec::with_capacity(headers.len());
        let mut content_length: isize = -1;
        let mut has_cl = false;
        let mut is_chunked = false;
        let mut keep_alive = version_opt.unwrap_or(1) == 1;
        let mut is_upgrade = false;
        let mut upgrade_proto = PyBytes::new(py, b"");
        let mut te_count = 0;
        let mut cl_conflict = false;

        for h in headers.iter() {
            if !h.name.is_empty() {
                let name_lower: Vec<u8> = h.name.bytes().map(|b| b.to_ascii_lowercase()).collect();
                let py_name = PyBytes::new(py, &name_lower);
                let py_val = PyBytes::new(py, h.value);

                if name_lower == b"content-length" {
                    if let Ok(s) = std::str::from_utf8(h.value) {
                        if let Ok(val) = s.trim().parse::<usize>() {
                            if has_cl && (val as isize) != content_length {
                                cl_conflict = true;
                            } else {
                                content_length = val as isize;
                                has_cl = true;
                            }
                        } else {
                            cl_conflict = true;
                        }
                    } else {
                        cl_conflict = true;
                    }
                } else if name_lower == b"connection" {
                    let v_lower = h.value.to_ascii_lowercase();
                    if v_lower.windows(5).any(|w| w == b"close")
                        || h.value.eq_ignore_ascii_case(b"close")
                    {
                        keep_alive = false;
                    } else if v_lower.windows(10).any(|w| w == b"keep-alive")
                        || h.value.eq_ignore_ascii_case(b"keep-alive")
                    {
                        keep_alive = true;
                    }
                    if v_lower.windows(7).any(|w| w == b"upgrade")
                        || h.value.eq_ignore_ascii_case(b"upgrade")
                    {
                        is_upgrade = true;
                    }
                } else if name_lower == b"upgrade" {
                    upgrade_proto = PyBytes::new(py, &h.value.to_ascii_lowercase());
                } else if name_lower == b"transfer-encoding" {
                    te_count += 1;
                    if let Ok(val_str) = std::str::from_utf8(h.value) {
                        let codings: Vec<&str> = val_str
                            .split(',')
                            .map(|s| s.trim())
                            .filter(|s| !s.is_empty())
                            .collect();
                        if let Some(&last_coding) = codings.last() {
                            if last_coding.eq_ignore_ascii_case("chunked") {
                                is_chunked = true;
                            } else {
                                cl_conflict = true;
                            }
                        } else {
                            cl_conflict = true;
                        }
                    } else {
                        cl_conflict = true;
                    }
                }

                let tuple = PyTuple::new(py, [py_name, py_val])?;
                header_tuples.push(tuple);
            }
        }

        if cl_conflict || (is_chunked && has_cl) || (te_count > 1 && !is_chunked) {
            return Err(PyValueError::new_err(
                "Bad Request: Conflicting HTTP headers",
            ));
        }

        let py_headers = PyList::new(py, header_tuples)?;

        Ok(Some((
            method,
            path_str,
            raw_path_bytes,
            query_bytes,
            version,
            py_headers,
            body_offset,
            content_length,
            keep_alive,
            is_chunked,
            is_upgrade,
            upgrade_proto,
        )))
    }
}
