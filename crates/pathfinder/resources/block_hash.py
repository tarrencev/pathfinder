import sys
import asyncio

from starkware.cairo.common.hash_state import compute_hash_on_elements
from starkware.cairo.lang.vm.crypto import pedersen_hash
from starkware.starknet.services.api.feeder_gateway.block_hash import (
    calculate_event_hash, calculate_single_tx_hash_with_signature, calculate_patricia_root)
from starkware.starknet.services.api.feeder_gateway.response_objects import \
    StarknetBlock
from starkware.storage.storage import FactFetchingContext
from starkware.storage.dict_storage import DictStorage
from starkware.python.utils import from_bytes, to_bytes


def main():
    with open(sys.argv[1]) as f:
        block = StarknetBlock.loads(f.read())
        print(len(block.transactions))

    print(hex(calculate_event_hash(0xdeadbeef, [1, 2, 3, 4], [5, 6, 7, 8, 9])))

    print(hex(compute_hash_on_elements([1, 2, 3, 4])))

    print(hex(calculate_single_tx_hash_with_signature(
        1, [2, 3], hash_function=pedersen_hash)))

    def bytes_hash_function(x: bytes, y: bytes) -> bytes:
        return to_bytes(pedersen_hash(from_bytes(x), from_bytes(y)))

    ffc = FactFetchingContext(storage=DictStorage(),
                              hash_func=bytes_hash_function)

    root = asyncio.run(calculate_patricia_root(
        [1, 2, 3, 4], height=64, ffc=ffc))
    print(hex(root))


if __name__ == "__main__":
    main()
