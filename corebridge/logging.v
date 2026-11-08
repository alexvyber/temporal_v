module corebridge

fn C.spike_log_line(&u8, u32)

pub fn log_line(line string) {
	C.spike_log_line(line.str, u32(line.len))
}
