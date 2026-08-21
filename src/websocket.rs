use pyo3::buffer::PyBuffer;
use pyo3::prelude::*;
use pyo3::types::PyBytes;

/// Fast SIMD/64-bit word WebSocket payload unmasking.
/// Allocates a single new `PyBytes` directly on the Python heap and performs
/// vector XOR unmasking into it, avoiding any intermediate buffers or unsafe in-place mutation.
#[pyfunction]
pub fn fast_websocket_unmask<'py>(
    py: Python<'py>,
    payload: &Bound<'py, PyAny>,
    mask: [u8; 4],
) -> PyResult<Bound<'py, PyBytes>> {
    let py_buf = PyBuffer::<u8>::get(payload)?;
    let slice =
        unsafe { std::slice::from_raw_parts(py_buf.buf_ptr() as *const u8, py_buf.len_bytes()) };
    fast_websocket_unmask_raw(py, slice, mask)
}

/// Raw slice SIMD/64-bit unmasking directly into a new PyBytes object.
pub fn fast_websocket_unmask_raw<'py>(
    py: Python<'py>,
    slice: &[u8],
    mask: [u8; 4],
) -> PyResult<Bound<'py, PyBytes>> {
    let len = slice.len();
    PyBytes::new_with(py, len, |out| {
        let mask_u64 = u64::from_ne_bytes([
            mask[0], mask[1], mask[2], mask[3], mask[0], mask[1], mask[2], mask[3],
        ]);

        let mut i = 0;
        // Process 32-byte blocks with 4x 64-bit integer XOR (auto-vectorized to AVX2/NEON)
        while i + 32 <= len {
            let c0 = u64::from_ne_bytes(match slice[i..i + 8].try_into() {
                Ok(b) => b,
                Err(_) => {
                    return Err(pyo3::exceptions::PyValueError::new_err(
                        "Invalid buffer chunk",
                    ))
                }
            }) ^ mask_u64;
            let c1 = u64::from_ne_bytes(match slice[i + 8..i + 16].try_into() {
                Ok(b) => b,
                Err(_) => {
                    return Err(pyo3::exceptions::PyValueError::new_err(
                        "Invalid buffer chunk",
                    ))
                }
            }) ^ mask_u64;
            let c2 = u64::from_ne_bytes(match slice[i + 16..i + 24].try_into() {
                Ok(b) => b,
                Err(_) => {
                    return Err(pyo3::exceptions::PyValueError::new_err(
                        "Invalid buffer chunk",
                    ))
                }
            }) ^ mask_u64;
            let c3 = u64::from_ne_bytes(match slice[i + 24..i + 32].try_into() {
                Ok(b) => b,
                Err(_) => {
                    return Err(pyo3::exceptions::PyValueError::new_err(
                        "Invalid buffer chunk",
                    ))
                }
            }) ^ mask_u64;
            out[i..i + 8].copy_from_slice(&c0.to_ne_bytes());
            out[i + 8..i + 16].copy_from_slice(&c1.to_ne_bytes());
            out[i + 16..i + 24].copy_from_slice(&c2.to_ne_bytes());
            out[i + 24..i + 32].copy_from_slice(&c3.to_ne_bytes());
            i += 32;
        }

        while i + 8 <= len {
            let chunk = u64::from_ne_bytes(match slice[i..i + 8].try_into() {
                Ok(b) => b,
                Err(_) => {
                    return Err(pyo3::exceptions::PyValueError::new_err(
                        "Invalid buffer chunk",
                    ))
                }
            });
            let unmasked = chunk ^ mask_u64;
            out[i..i + 8].copy_from_slice(&unmasked.to_ne_bytes());
            i += 8;
        }

        // Handle trailing 1-7 bytes
        while i < len {
            out[i] = slice[i] ^ mask[i % 4];
            i += 1;
        }
        Ok(())
    })
}

/// Fast SIMD/64-bit slice WebSocket payload unmasking directly from buffer offset.
#[pyfunction]
pub fn fast_websocket_unmask_slice<'py>(
    py: Python<'py>,
    payload: &Bound<'py, PyAny>,
    offset: usize,
    length: usize,
    mask: [u8; 4],
) -> PyResult<Bound<'py, PyBytes>> {
    let py_buf = PyBuffer::<u8>::get(payload)?;
    let total_len = py_buf.len_bytes();
    if offset.checked_add(length).is_none_or(|end| end > total_len) {
        return Err(pyo3::exceptions::PyValueError::new_err(
            "slice out of bounds",
        ));
    }
    let slice = unsafe {
        let ptr = (py_buf.buf_ptr() as *const u8).add(offset);
        std::slice::from_raw_parts(ptr, length)
    };
    PyBytes::new_with(py, length, |out| {
        let mask_u64 = u64::from_ne_bytes([
            mask[0], mask[1], mask[2], mask[3], mask[0], mask[1], mask[2], mask[3],
        ]);

        let mut i = 0;
        // Process 32-byte blocks with 4x 64-bit integer XOR
        while i + 32 <= length {
            let c0 = u64::from_ne_bytes(match slice[i..i + 8].try_into() {
                Ok(b) => b,
                Err(_) => {
                    return Err(pyo3::exceptions::PyValueError::new_err(
                        "Invalid buffer chunk",
                    ))
                }
            }) ^ mask_u64;
            let c1 = u64::from_ne_bytes(match slice[i + 8..i + 16].try_into() {
                Ok(b) => b,
                Err(_) => {
                    return Err(pyo3::exceptions::PyValueError::new_err(
                        "Invalid buffer chunk",
                    ))
                }
            }) ^ mask_u64;
            let c2 = u64::from_ne_bytes(match slice[i + 16..i + 24].try_into() {
                Ok(b) => b,
                Err(_) => {
                    return Err(pyo3::exceptions::PyValueError::new_err(
                        "Invalid buffer chunk",
                    ))
                }
            }) ^ mask_u64;
            let c3 = u64::from_ne_bytes(match slice[i + 24..i + 32].try_into() {
                Ok(b) => b,
                Err(_) => {
                    return Err(pyo3::exceptions::PyValueError::new_err(
                        "Invalid buffer chunk",
                    ))
                }
            }) ^ mask_u64;
            out[i..i + 8].copy_from_slice(&c0.to_ne_bytes());
            out[i + 8..i + 16].copy_from_slice(&c1.to_ne_bytes());
            out[i + 16..i + 24].copy_from_slice(&c2.to_ne_bytes());
            out[i + 24..i + 32].copy_from_slice(&c3.to_ne_bytes());
            i += 32;
        }

        while i + 8 <= length {
            let chunk = u64::from_ne_bytes(match slice[i..i + 8].try_into() {
                Ok(b) => b,
                Err(_) => {
                    return Err(pyo3::exceptions::PyValueError::new_err(
                        "Invalid buffer chunk",
                    ))
                }
            });
            let unmasked = chunk ^ mask_u64;
            out[i..i + 8].copy_from_slice(&unmasked.to_ne_bytes());
            i += 8;
        }

        while i < length {
            out[i] = slice[i] ^ mask[i % 4];
            i += 1;
        }
        Ok(())
    })
}

pub(crate) type ParsedWebSocketFrameHeader = (u8, bool, bool, usize, Option<[u8; 4]>, usize);

/// Zero-copy RFC 6455 WebSocket frame header parser.
#[pyclass(module = "multiloop._multiloop_core")]
pub struct FastWebSocketParser;

#[pymethods]
impl FastWebSocketParser {
    /// Parses the frame header from the byte slice.
    /// Returns: `Option<(opcode, fin, masked, payload_len, Option<[u8; 4]>, header_len)>`
    #[staticmethod]
    pub fn parse_frame_header(
        buf_obj: &Bound<'_, PyAny>,
    ) -> PyResult<Option<ParsedWebSocketFrameHeader>> {
        let py_buf = PyBuffer::<u8>::get(buf_obj)?;
        let buf = unsafe {
            std::slice::from_raw_parts(py_buf.buf_ptr() as *const u8, py_buf.len_bytes())
        };
        if buf.len() < 2 {
            return Ok(None);
        }

        let b1 = buf[0];
        let b2 = buf[1];

        let fin = (b1 & 0x80) != 0;
        let opcode = b1 & 0x0F;
        let masked = (b2 & 0x80) != 0;
        let raw_len = (b2 & 0x7F) as usize;

        let (payload_len, mut offset) = match raw_len {
            126 => {
                if buf.len() < 4 {
                    return Ok(None);
                }
                let len = u16::from_be_bytes([buf[2], buf[3]]) as usize;
                (len, 4)
            }
            127 => {
                if buf.len() < 10 {
                    return Ok(None);
                }
                let len = u64::from_be_bytes(buf[2..10].try_into().unwrap()) as usize;
                (len, 10)
            }
            len => (len, 2),
        };

        let mask_key = if masked {
            if buf.len() < offset + 4 {
                return Ok(None);
            }
            let key = [
                buf[offset],
                buf[offset + 1],
                buf[offset + 2],
                buf[offset + 3],
            ];
            offset += 4;
            Some(key)
        } else {
            None
        };

        Ok(Some((opcode, fin, masked, payload_len, mask_key, offset)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fast_websocket_unmask() {
        Python::attach(|py| {
            let mask = [0x12, 0x34, 0x56, 0x78];
            let original = b"Hello, WebSocket World with SIMD acceleration!";
            let mut masked = original.to_vec();
            for (i, b) in masked.iter_mut().enumerate() {
                *b ^= mask[i % 4];
            }

            let py_buf = PyBytes::new(py, &masked);
            let unmasked = fast_websocket_unmask(py, &py_buf, mask).unwrap();
            assert_eq!(unmasked.as_bytes(), original);
        });
    }

    #[test]
    fn test_parse_frame_header() {
        Python::attach(|py| {
            // Unmasked single-byte payload text frame: 0x81 (FIN + Text), 0x05, "hello"
            let frame = [0x81, 0x05, b'h', b'e', b'l', b'l', b'o'];
            let py_buf = PyBytes::new(py, &frame);
            let (opcode, fin, masked, len, mask_key, header_len) =
                FastWebSocketParser::parse_frame_header(&py_buf)
                    .unwrap()
                    .unwrap();
            assert_eq!(opcode, 1);
            assert!(fin);
            assert!(!masked);
            assert_eq!(len, 5);
            assert!(mask_key.is_none());
            assert_eq!(header_len, 2);
        });
    }
}
