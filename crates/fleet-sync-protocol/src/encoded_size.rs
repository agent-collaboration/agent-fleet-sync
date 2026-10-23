use serde::Serialize;

/// Count the exact compact JSON bytes without allocating an encoded copy.
pub fn encoded_json_size<T: Serialize + ?Sized>(value: &T) -> Result<usize, serde_json::Error> {
    let mut counter = JsonBytes(0);
    serde_json::to_writer(&mut counter, value)?;
    Ok(counter.0)
}

struct JsonBytes(usize);
impl std::io::Write for JsonBytes {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0 = self.0.saturating_add(bytes.len());
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
