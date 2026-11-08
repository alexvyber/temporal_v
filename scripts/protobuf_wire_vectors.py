"""Deterministic malformed/valid wire corpus for the pinned Rust/V decoders."""
import random

def varint(value):
    out = bytearray()
    while value >= 128:
        out.append(value & 127 | 128)
        value >>= 7
    out.append(value)
    return bytes(out)

def blob(field, data):
    return varint(field * 8 + 2) + varint(len(data)) + data

def wire_vectors():
    vectors = []

    def add(kind, data, label):
        vectors.append((kind, data, label))
    seeds = {'workflow': [b'', blob(1, b'hello') + blob(2, '世界'.encode()), blob(1, b'x') * 2], 'payload': [b'', blob(1, blob(1, b'encoding') + blob(2, b'json/plain')) + blob(2, b'\x00\xff')], 'reach': [b'', blob(1, b'queue') + blob(2, bytes([1, 2, 3])), bytes([16, 1, 16, 2, 16, 3])], 'link': [b'', blob(1, blob(1, b'namespace') + blob(2, b'workflow'))], 'failure': [b'', blob(1, b'failure'), blob(4, blob(1, b'cause'))]}
    rng = random.Random(14819)
    for (kind, items) in seeds.items():
        for seed in items:
            add(kind, seed, 'valid seed')
            for size in range(len(seed)):
                add(kind, seed[:size], 'truncation')
            for _ in range(120):
                value = bytearray(seed)
                for _ in range(rng.randint(1, 4)):
                    if value and rng.choice([True, False]):
                        value[rng.randrange(len(value))] = rng.randrange(256)
                    else:
                        value.insert(rng.randrange(len(value) + 1), rng.randrange(256))
                add(kind, bytes(value), 'mutation')
        for _ in range(150):
            add(kind, rng.randbytes(rng.randrange(0, 34)), 'random')
        for field in (100, 101):
            add(kind, varint(field * 8 + 2) + b'\x02\x00\xff', 'unknown bytes')
            for count in (1, 2, 98, 99, 100, 101, 102):
                add(kind, varint(field * 8 + 3) * count + bytes([8, 1]) + varint(field * 8 + 4) * count, 'unknown group depth ' + str(count))
                add(kind, varint(field * 8 + 3) * count + varint(field * 8 + 4) * count, 'empty unknown group depth ' + str(count))
            add(kind, varint(field * 8 + 3) + varint((field + 1) * 8 + 4), 'mismatched group')
    for depth in (1, 2, 98, 99, 100, 101, 102, 130):
        data = blob(1, b'leaf')
        for _ in range(depth):
            data = blob(4, data)
        add('failure', data, 'message depth ' + str(depth))
    for value in (0, 1, 127, 128, 2 ** 32 - 1, 2 ** 63, 2 ** 64 - 1):
        add('varint', varint(value), 'varint boundary')
    for last in (0, 1, 2, 127, 128, 255):
        add('varint', b'\xff' * 9 + bytes([last]), 'varint tenth byte')
    for kind in seeds:
        for size in range(2, 11):
            add(kind, b'\x8a' + b'\x80' * (size - 2) + b'\x00\x00', 'noncanonical tag ' + str(size))
        add(kind, b'\n\x01\xff', 'invalid UTF-8 known string')
        add(kind, b'\x08\x01A', 'wrong known field wire')
        add(kind, b'\x80\x80\x80\x80\x80\x00', 'overlong tag zero')
    return vectors
