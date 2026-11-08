module sdk

import corebridge

// A token may be shared by concurrent RPCs. It is independent of the client:
// cancelling a result/Update wait does not cancel the workflow or roll back a
// request already accepted by Temporal. The caller owns closing the token.
@[heap]
pub struct RpcCancellation {
	token &corebridge.RpcCancellation
}

pub fn new_rpc_cancellation() !&RpcCancellation {
	return &RpcCancellation{
		token: corebridge.new_rpc_cancellation()!
	}
}

pub fn (cancellation &RpcCancellation) cancel() {
	cancellation.token.cancel()
}

pub fn (cancellation &RpcCancellation) close() {
	cancellation.token.close()
}
