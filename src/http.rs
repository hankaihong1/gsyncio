use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyList, PyString, PyTuple};
use std::sync::OnceLock;

struct InternedMethods {
    get: Py<PyString>,
    post: Py<PyString>,
    put: Py<PyString>,
    delete: Py<PyString>,
    patch: Py<PyString>,
    head: Py<PyString>,
    options: Py<PyString>,
    v10: Py<PyString>,
    v11: Py<PyString>,
    v20: Py<PyString>,
}

static INTERNED: OnceLock<InternedMethods> = OnceLock::new();

fn get_interned(py: Python<'_>) -> &InternedMethods {
    INTERNED.get_or_init(|| InternedMethods {
        get: PyString::new(py, "GET").unbind(),
        post: PyString::new(py, "POST").unbind(),
        put: PyString::new(py, "PUT").unbind(),
        delete: PyString::new(py, "DELETE").unbind(),
        patch: PyString::new(py, "PATCH").unbind(),
        head: PyString::new(py, "HEAD").unbind(),
        options: PyString::new(py, "OPTIONS").unbind(),
        v10: PyString::new(py, "1.0").unbind(),
        v11: PyString::new(py, "1.1").unbind(),
        v20: PyString::new(py, "2.0").unbind(),
    })
}

pub(crate) type ParsedHttpRequest<'py> = (
    Bound<'py, PyString>,
    String,
    String,
    Bound<'py, PyString>,
    Bound<'py, PyList>,
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
    pub fn parse_request<'py>(
        py: Python<'py>,
        buf: &[u8],
    ) -> PyResult<Option<ParsedHttpRequest<'py>>> {
        let mut headers = [httparse::EMPTY_HEADER; 64];
        let mut req = httparse::Request::new(&mut headers);
        match req.parse(buf) {
            Ok(httparse::Status::Complete(body_offset)) => {
                let interned = get_interned(py);
                let method = match req.method.unwrap_or("GET") {
                    "GET" => interned.get.bind(py).clone(),
                    "POST" => interned.post.bind(py).clone(),
                    "PUT" => interned.put.bind(py).clone(),
                    "DELETE" => interned.delete.bind(py).clone(),
                    "PATCH" => interned.patch.bind(py).clone(),
                    "HEAD" => interned.head.bind(py).clone(),
                    "OPTIONS" => interned.options.bind(py).clone(),
                    m => PyString::new(py, m),
                };

                let full_path = req.path.unwrap_or("/");
                let (path, query_string) = match full_path.split_once('?') {
                    Some((p, q)) => (p.to_string(), q.to_string()),
                    None => (full_path.to_string(), String::new()),
                };

                let version = match req.version.unwrap_or(1) {
                    0 => interned.v10.bind(py).clone(),
                    1 => interned.v11.bind(py).clone(),
                    2 => interned.v20.bind(py).clone(),
                    v => PyString::new(py, &format!("1.{}", v)),
                };

                let py_headers = PyList::empty(py);
                for h in req.headers.iter() {
                    if !h.name.is_empty() {
                        let name_bytes = h.name.to_ascii_lowercase().into_bytes();
                        let py_name = PyBytes::new(py, &name_bytes);
                        let py_val = PyBytes::new(py, h.value);
                        let tuple = PyTuple::new(py, [py_name, py_val])?;
                        py_headers.append(tuple)?;
                    }
                }

                Ok(Some((
                    method,
                    path,
                    query_string,
                    version,
                    py_headers,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fast_http_parser_simd_and_interning() {
        Python::attach(|py| {
            let req_data = b"GET /api/v1/items?search=rust HTTP/1.1\r\nHost: localhost:8000\r\nAccept: */*\r\n\r\n";
            let res = FastHttpParser::parse_request(py, req_data).unwrap();
            assert!(res.is_some());
            let (method, path, query_string, version, headers, body_offset) = res.unwrap();
            assert_eq!(method.to_str().unwrap(), "GET");
            assert_eq!(path, "/api/v1/items");
            assert_eq!(query_string, "search=rust");
            assert_eq!(version.to_str().unwrap(), "1.1");
            assert_eq!(headers.len(), 2);
            assert_eq!(body_offset, req_data.len());
        });
    }
}
