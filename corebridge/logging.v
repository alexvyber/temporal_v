module corebridge

fn C.spike_log_line(&u8, uint)

pub fn log_line(line string) {
	C.spike_log_line(line.str, uint(line.len))
}
