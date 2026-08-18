use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;

pub(crate) type ParsedHttpRequest = (
    String,
    String,
    String,
    String,
    Vec<(Vec<u8>, Vec<u8>)>,
    usize,
);

/// Zero-copy HTTP/1.x request header parser powered by SIMD httparse.
#[pyclass(module = "multiloop._multiloop_core")]
pub struct FastHttpParser;

#[pymethods]
impl FastHttpParser {
    /// Parses raw HTTP request byte buffer into structured metadata.
    /// Returns: Option<(method, path, query_string, version, headers, body_offset)>
    #[staticmethod]
    pub fn parse_request(buf: &[u8]) -> PyResult<Option<ParsedHttpRequest>> {
        let mut headers = [httparse::EMPTY_HEADER; 64];
        let mut req = httparse::Request::new(&mut headers);
        match req.parse(buf) {
            Ok(httparse::Status::Complete(body_offset)) => {
                let method = req.method.unwrap_or("GET").to_string();
                let full_path = req.path.unwrap_or("/");
                let (path, query_string) = match full_path.split_once('?') {
                    Some((p, q)) => (p.to_string(), q.to_string()),
                    None => (full_path.to_string(), String::new()),
                };
                let version = match req.version.unwrap_or(1) {
                    0 => "1.0".to_string(),
                    1 => "1.1".to_string(),
                    v => format!("1.{}", v),
                };
                let mut parsed_headers = Vec::with_capacity(req.headers.len());
                for h in req.headers.iter() {
                    if !h.name.is_empty() {
                        parsed_headers
                            .push((h.name.to_ascii_lowercase().into_bytes(), h.value.to_vec()));
                    }
                }
                Ok(Some((
                    method,
                    path,
                    query_string,
                    version,
                    parsed_headers,
                    body_offset,
                )))
            }
            Ok(httparse::Status::Partial) => Ok(None),
            Err(e) => Err(PyRuntimeError::new_err(format!(
                "Malformed HTTP request: {}",
                e
            ))),
        }
    }
}
