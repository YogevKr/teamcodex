use serde_json::Value;

// Observation is bounded. Bytes always pass through independently of parsing.
const MAX_EVENT: usize = 4 * 1024 * 1024;

#[derive(Default)]
pub struct Parser {
    line: Vec<u8>,
    data: Vec<u8>,
    discard: bool,
    pub overflowed: bool,
}

impl Parser {
    pub fn feed(&mut self, bytes: &[u8]) -> Vec<Value> {
        let mut events = Vec::new();
        for &byte in bytes {
            if byte == b'\n' {
                if self.line.last() == Some(&b'\r') {
                    self.line.pop();
                }
                if self.line.is_empty() {
                    if !self.discard
                        && !self.data.is_empty()
                        && let Ok(event) = serde_json::from_slice(&self.data)
                    {
                        events.push(event);
                    }
                    self.data.clear();
                    self.discard = false;
                } else if !self.discard
                    && let Some(data) = self.line.strip_prefix(b"data:")
                {
                    if !self.data.is_empty() {
                        self.data.push(b'\n');
                    }
                    self.data
                        .extend_from_slice(data.strip_prefix(b" ").unwrap_or(data));
                }
                self.line.clear();
            } else if !self.discard {
                self.line.push(byte);
            } else if self.line.is_empty() {
                // Preserve whether a discarded line was nonempty until its newline.
                self.line.push(b'x');
            }
            if self.line.len() + self.data.len() > MAX_EVENT {
                self.discard = true;
                self.overflowed = true;
                self.line.clear();
                self.line.push(b'x');
                self.data.clear();
            }
        }
        events
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn every_byte_boundary_and_unicode() {
        let data = "event: response.completed\r\ndata: {\"type\":\"response.completed\",\"text\":\"שלום\"}\r\n\r\n".as_bytes();
        for boundary in 0..=data.len() {
            let mut parser = Parser::default();
            let mut events = parser.feed(&data[..boundary]);
            events.extend(parser.feed(&data[boundary..]));
            assert_eq!(events.len(), 1, "boundary {boundary}");
            assert_eq!(events[0]["text"], "שלום");
        }
    }
    #[test]
    fn oversized_event_recovers() {
        let mut p = Parser::default();
        p.feed(&vec![b'x'; MAX_EVENT + 20]);
        let events = p.feed(b"\n\ndata: {\"type\":\"ok\"}\n\n");
        assert!(p.overflowed);
        assert_eq!(events.len(), 1);
    }
}
