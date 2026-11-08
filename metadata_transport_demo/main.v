module main

import alexvyber.temporal_v.sdk
import alexvyber.temporal_v.corebridge
import alexvyber.temporal_v.temporal
import encoding.base64
import sync

@[heap]
struct ReceivedMetadata {
	mutex &sync.Mutex = sync.new_mutex()
mut:
	calls int
}

struct CollectMetadata {
	received &ReceivedMetadata
}

fn (transport CollectMetadata) call(context &sdk.GrpcTransportContext, request corebridge.GrpcRequestSnapshot) !corebridge.GrpcReply {
	mut ascii := []string{}
	mut binary := [][]u8{}
	mut check := false
	for header in request.headers {
		if header.name == 'x-mode' { check = true }
		if header.name == 'x-repeat' { ascii << header.value.bytestr() }
		if header.name == 'x-repeat-bin' {
			for part in header.value.bytestr().split(',') {
				mut encoded := part.trim_space()
				for encoded.len % 4 != 0 {
					encoded += '='
				}
				binary << base64.decode(encoded)
			}
		}
	}
	if check {
		if ascii != ['one', 'two'] || binary != [[u8(0), 255], [u8(128), 10], []u8{}] {
			eprintln('METADATA_OBSERVED ascii=' + ascii.str() + ' binary=' + binary.str())
			return error('repeated metadata was lost or reordered: ' + ascii.str() + ' ' +
				binary.str())
		}
		mut received := transport.received
		received.mutex.lock()
		received.calls++
		received.mutex.unlock()
	}
	return corebridge.GrpcReply{
		body:    temporal.TemporalApiWorkflowserviceV1_GetSystemInfoResponse{
			server_version: 'multiplicity'
		}.encode()
		headers: [
			corebridge.GrpcHeader{
				name:  'x-response'
				value: 'one'.bytes()
			},
			corebridge.GrpcHeader{
				name:  'x-response'
				value: 'two'.bytes()
			},
		]
	}
}

struct ForwardMetadata {
	backend  &sdk.Client
	coalesce bool
}

fn (transport ForwardMetadata) call(supplied &sdk.GrpcTransportContext, request corebridge.GrpcRequestSnapshot) !corebridge.GrpcReply {
	mut context := supplied
	mut headers := request.headers.clone()
	if transport.coalesce {
		mut combined := []string{}
		mut retained := []corebridge.GrpcHeader{}
		for header in headers {
			if header.name == 'x-repeat-bin' {
				combined << header.value.bytestr()
			} else {
				retained << header
			}
		}
		if combined.len > 0 {
			retained << corebridge.GrpcHeader{
				name:  'x-repeat-bin'
				value: combined.join(', ').bytes()
			}
		}
		headers = retained.clone()
	}
	return context.forward(transport.backend, corebridge.GrpcRequestSnapshot{
		...request
		headers: headers
	}, 5000)!
}

fn request(client &sdk.Client) ! {
	response := client.call_rpc(sdk.ClientRpcRequest{
		service:  .workflow
		method:   'GetSystemInfo'
		metadata: [
			corebridge.RpcMetadataEntry{
				name:  'x-mode'
				value: 'check'.bytes()
			},
			corebridge.RpcMetadataEntry{
				name:  'x-repeat'
				value: 'one'.bytes()
			},
			corebridge.RpcMetadataEntry{
				name:  'x-repeat'
				value: 'two'.bytes()
			},
			corebridge.RpcMetadataEntry{
				name:  'x-repeat-bin'
				value: [u8(0), 255]
			},
			corebridge.RpcMetadataEntry{
				name:  'x-repeat-bin'
				value: [u8(128), 10]
			},
			corebridge.RpcMetadataEntry{
				name:  'x-repeat-bin'
				value: []u8{}
			},
		]
	})!
	if temporal.TemporalApiWorkflowserviceV1_GetSystemInfoResponse.decode(response)!.server_version != 'multiplicity' {
		return error('metadata request result changed')
	}
}

fn run() ! {
	received := &ReceivedMetadata{}
	backend := sdk.connect(sdk.ClientOptions{
		address:   'http://metadata.invalid:7233'
		transport: sdk.TransportOptions{
			metadata:        {
				'x-repeat': 'connection-default'
			}
			binary_metadata: {
				'x-repeat-bin': [u8(127)]
			}
			custom_grpc:     sdk.CustomGrpcTransport{
				transport: CollectMetadata{
					received: received
				}
			}
		}
	})!
	defer { backend.close() }
	request(backend)!
	for coalesce in [false, true] {
		client := sdk.connect(sdk.ClientOptions{
			address:   'http://forward.invalid:7233'
			transport: sdk.TransportOptions{
				custom_grpc: sdk.CustomGrpcTransport{
					transport: ForwardMetadata{
						backend:  backend
						coalesce: coalesce
					}
				}
			}
		})!
		request(client)!
		results := chan string{cap: 8}
		for _ in 0 .. 8 {
			spawn metadata_burst(client, results)
		}
		for _ in 0 .. 8 {
			if <-results != '' { return error('concurrent metadata request failed') }
		}
		client.close()
	}
	received.mutex.lock()
	calls := received.calls
	received.mutex.unlock()
	if calls != 163 { return error('metadata calls missing') }
	for entry in [
		corebridge.RpcMetadataEntry{
			name:  'bad key'
			value: 'secret'.bytes()
		},
		corebridge.RpcMetadataEntry{
			name:  'ascii'
			value: [u8(255)]
		},
		corebridge.RpcMetadataEntry{
			name:  'ascii'
			value: 'bad\nvalue'.bytes()
		},
	] {
		if _ := backend.call_rpc(sdk.ClientRpcRequest{
			service:  .workflow
			method:   'GetSystemInfo'
			metadata: [
				entry,
			]
		})
		{
			return error('invalid metadata accepted')
		}
	}
	println('METADATA_MULTIPLICITY_OK')
}

fn main() {
	run() or {
		eprintln(err.msg())
		exit(1)
	}
}

fn metadata_burst(client &sdk.Client, results chan string) {
	for _ in 0 .. 10 {
		request(client) or {
			results <- err.msg()
			return
		}
	}
	results <- ''
}
