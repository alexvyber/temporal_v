module corebridge

@[typedef]
struct C.SpikeServerOptions {
	executable             &char
	namespace_name         &char
	ip                     &char
	database_filename      &char
	extra_args             &char
	port                   u16
	startup_timeout_millis u64
	time_skipping          bool
}

fn C.spike_server_open(&C.SpikeServerOptions) &C.SpikeCore
fn C.spike_server_target(&C.SpikeCore) &char

pub struct ServerOptions {
pub:
	executable             string
	namespace              string = 'default'
	ip                     string = '127.0.0.1'
	database_filename      string
	extra_args             []string
	port                   u16
	startup_timeout_millis u64 = 5000
	time_skipping          bool
}

pub struct Server {
	connection &Connection
pub:
	address string
}

pub fn start_server(options ServerOptions) !Server {
	for name, value in {
		'executable':        options.executable
		'namespace':         options.namespace
		'IP':                options.ip
		'database filename': options.database_filename
	} {
		validate_text(value, name)!
	}
	for argument in options.extra_args {
		validate_text(argument, 'server argument')!
	}
	if options.executable == '' || options.namespace == '' || options.startup_timeout_millis == 0 {
		return error('invalid server options')
	}
	for argument in options.extra_args {
		if argument.contains('\n') { return error('server arguments cannot contain newlines') }
	}
	arguments := options.extra_args.join('\n')
	native := C.SpikeServerOptions{
		executable:             options.executable.str
		namespace_name:         options.namespace.str
		ip:                     options.ip.str
		database_filename:      options.database_filename.str
		extra_args:             arguments.str
		port:                   options.port
		startup_timeout_millis: options.startup_timeout_millis
		time_skipping:          options.time_skipping
	}
	core := C.spike_server_open(&native)
	if isnil(core) { return error('cannot allocate test server runtime') }
	connection := &Connection{
		core: core
	}
	connection.check() or {
		C.spike_close(core)
		return err
	}
	address := unsafe { C.spike_server_target(core).vstring().clone() }
	return Server{
		address:    'http://' + address
		connection: connection
	}
}

pub fn (server &Server) close() {
	server.connection.close()
}
