"""Inspect explicitly labelled recovery-boundary traces in actual server failures."""
def assert_stack(failure, expected):
    trace=failure.get('stackTrace','')
    assert trace.startswith('Recovered panic; recovery boundary stack'), failure
    assert 0 < len(trace.encode()) <= 16384, len(trace)

def assert_panic_histories(fixture,handles):
    checked=0
    for handle,expected in handles:
        history=fixture.history(handle)
        failures=[]
        def visit(value):
            if isinstance(value,dict):
                if value.get('message') in ('Activity panic','start panic','signal panic','update handler panic','command interceptor panic','factory panic') and 'stackTrace' in value:failures.append(value)
                for child in value.values():visit(child)
            elif isinstance(value,list):
                for child in value:visit(child)
        visit(history)
        assert failures,(handle,history)
        for failure in failures:assert_stack(failure,expected);checked+=1
    return checked
