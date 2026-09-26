use serde_json::Value;

// Buffer one bounded event so the proxy can replace a terminal overload error
// before any of its bytes reach Codex. Other events retain their exact bytes.
const MAX_EVENT: usize = 4 * 1024 * 1024;

pub struct Frame {
    pub bytes: Vec<u8>,
    pub event: Option<Value>,
}

#[derive(Default)]
pub struct Parser {
    pending: Vec<u8>,
    line: Vec<u8>,
    data: Vec<u8>,
    discard: bool,
    pub overflowed: bool,
}

impl Parser {
    pub fn feed(&mut self, bytes: &[u8]) -> Vec<Frame> {
        let mut events = Vec::new();
        for &byte in bytes {
            self.pending.push(byte);
            if byte == b'\n' {
                if self.line.last() == Some(&b'\r') {
                    self.line.pop();
                }
                if self.line.is_empty() {
                    let event = if self.discard {
                        None
                    } else {
                        serde_json::from_slice(&self.data).ok()
                    };
                    events.push(Frame {
                        bytes: self.finish(),
                        event,
                    });
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
            } else if self.line.is_empty() && byte != b'\r' {
                // Preserve whether a discarded line was nonempty until its newline.
                self.line.push(b'x');
            }
            if self.pending.len() >= MAX_EVENT {
                self.discard = true;
                self.overflowed = true;
                if !self.line.is_empty() {
                    self.line.clear();
                    self.line.push(b'x');
                }
                self.data.clear();
                events.push(Frame {
                    bytes: self.finish(),
                    event: None,
                });
            }
        }
        events
    }

    pub fn finish(&mut self) -> Vec<u8> {
        std::mem::take(&mut self.pending)
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
            assert_eq!(events[0].event.as_ref().unwrap()["text"], "שלום");
            assert_eq!(events[0].bytes, data);
        }
    }
    #[test]
    fn oversized_event_recovers() {
        for separator in ["\n\n", "\r\n\r\n"] {
            let mut p = Parser::default();
            let mut data = vec![b'x'; MAX_EVENT + 20];
            data.extend_from_slice(
                format!("{separator}data: {{\"type\":\"ok\"}}{separator}").as_bytes(),
            );
            let events = p.feed(&data);
            assert!(p.overflowed);
            assert_eq!(
                events
                    .iter()
                    .flat_map(|frame| frame.bytes.iter().copied())
                    .collect::<Vec<_>>(),
                data
            );
            let parsed: Vec<_> = events
                .iter()
                .filter_map(|frame| frame.event.as_ref())
                .collect();
            assert_eq!(parsed.len(), 1);
            assert_eq!(parsed[0]["type"], "ok");
        }
    }

    #[test]
    fn mixed_frames_and_unfinished_tail_preserve_bytes() {
        let data = b": keepalive\r\n\r\ndata: broken\n\ndata: {\n data ignored\ndata: \"type\":\"ok\"}\n\ndata: [DONE]\n\npartial";
        for size in 1..=data.len() {
            let mut parser = Parser::default();
            let mut frames = Vec::new();
            for chunk in data.chunks(size) {
                frames.extend(parser.feed(chunk));
            }
            let mut output: Vec<_> = frames
                .iter()
                .flat_map(|frame| frame.bytes.iter().copied())
                .collect();
            output.extend(parser.finish());
            assert_eq!(output, data);
            assert_eq!(
                frames
                    .iter()
                    .filter_map(|frame| frame.event.as_ref())
                    .count(),
                1
            );
        }
    }
}
