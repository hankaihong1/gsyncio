use pyo3::prelude::*;
use pyo3::types::PyBytes;

/// Fast SIMD/64-bit word WebSocket payload unmasking.
/// Allocates a single new `PyBytes` directly on the Python heap and performs
/// vector XOR unmasking into it, avoiding any intermediate buffers or unsafe in-place mutation.
#[pyfunction]
pub fn fast_websocket_unmask<'py>(
    py: Python<'py>,
    payload: &[u8],
    mask: [u8; 4],
) -> PyResult<Bound<'py, PyBytes>> {
    let len = payload.len();
    PyBytes::new_with(py, len, |out| {
        let mask_u64 = u64::from_ne_bytes([
            mask[0], mask[1], mask[2], mask[3], mask[0], mask[1], mask[2], mask[3],
        ]);

        let mut i = 0;
        // Process 8-byte blocks with 64-bit integer XOR (auto-vectorized to AVX2/NEON)
        while i + 8 <= len {
            let chunk = u64::from_ne_bytes(payload[i..i + 8].try_into().unwrap());
            let unmasked = chunk ^ mask_u64;
            out[i..i + 8].copy_from_slice(&unmasked.to_ne_bytes());
            i += 8;
        }

        // Handle trailing 1-7 bytes
        while i < len {
            out[i] = payload[i] ^ mask[i % 4];
            i += 1;
        }
        Ok(())
    })
}

pub(crate) type ParsedWebSocketFrameHeader = (u8, bool, bool, usize, Option<Vec<u8>>, usize);

/// Zero-copy RFC 6455 WebSocket frame header parser.
#[pyclass(module = "multiloop._multiloop_core")]
pub struct FastWebSocketParser;

#[pymethods]
impl FastWebSocketParser {
    /// Parses the frame header from the byte slice.
    /// Returns: Option<(opcode, fin, masked, payload_len, Option<Vec<u8>>, header_len)>
    #[staticmethod]
    pub fn parse_frame_header(buf: &[u8]) -> Option<ParsedWebSocketFrameHeader> {
        if buf.len() < 2 {
            return None;
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
                    return None;
                }
                let len = u16::from_be_bytes([buf[2], buf[3]]) as usize;
                (len, 4)
            }
            127 => {
                if buf.len() < 10 {
                    return None;
                }
                let len = u64::from_be_bytes(buf[2..10].try_into().unwrap()) as usize;
                (len, 10)
            }
            len => (len, 2),
        };

        let mask_key = if masked {
            if buf.len() < offset + 4 {
                return None;
            }
            let key = buf[offset..offset + 4].to_vec();
            offset += 4;
            Some(key)
        } else {
            None
        };

        Some((opcode, fin, masked, payload_len, mask_key, offset))
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

            let unmasked = fast_websocket_unmask(py, &masked, mask).unwrap();
            assert_eq!(unmasked.as_bytes(), original);
        });
    }

    #[test]
    fn test_parse_frame_header() {
        // Unmasked single-byte payload text frame: 0x81 (FIN + Text), 0x05, "hello"
        let frame = [0x81, 0x05, b'h', b'e', b'l', b'l', b'o'];
        let (opcode, fin, masked, len, mask_key, header_len) =
            FastWebSocketParser::parse_frame_header(&frame).unwrap();
        assert_eq!(opcode, 1);
        assert!(fin);
        assert!(!masked);
        assert_eq!(len, 5);
        assert!(mask_key.is_none());
        assert_eq!(header_len, 2);
    }
}
