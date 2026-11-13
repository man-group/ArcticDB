"""
Copyright 2026 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import contextlib
import multiprocessing
import select
import socket
import threading


def _forward(source, destination, pattern, flip_reporter=None):
    """Copies one direction of a connection. With `flip`, flips one bit of the first copy of `pattern` on this
    connection. The last len(pattern) - 1 bytes are held back until more arrive, so a pattern split across two reads
    is still found."""
    flip = flip_reporter is not None
    held = b""
    try:
        while True:
            if not select.select([source], [], [], 0.05)[0]:
                # A keep-alive response never ends with EOF, so release the held-back bytes once the stream goes quiet
                destination.sendall(held)
                held = b""
                continue
            chunk = source.recv(65536)
            if not chunk:
                destination.sendall(held)
                return
            data = bytearray(held + chunk)
            if flip and (offset := data.find(pattern)) >= 0:
                data[offset] ^= 0x01
                flip = False
                flip_reporter()
            split = max(len(data) - (len(pattern) - 1 if flip else 0), 0)
            destination.sendall(bytes(data[:split]))
            held = bytes(data[split:])
    except OSError:
        pass
    finally:
        source.close()
        destination.close()


def _serve(target_port, pattern, sender):
    # The first message on `sender` is the port; each later one reports a flip
    listener = socket.create_server(("127.0.0.1", 0))
    sender.send(listener.getsockname()[1])
    send_lock = threading.Lock()

    def report_flip():
        with send_lock:
            sender.send(True)

    while True:
        client, _ = listener.accept()
        server = socket.create_connection(("127.0.0.1", target_port))
        threading.Thread(target=_forward, args=(client, server, pattern), daemon=True).start()
        threading.Thread(target=_forward, args=(server, client, pattern, report_flip), daemon=True).start()


@contextlib.contextmanager
def byte_flipping_proxy(target_port, pattern):
    """A TCP proxy to 127.0.0.1:`target_port` that flips one bit of the first copy of `pattern` in the responses of
    each connection, as damage in transit would. Yields the proxy's port and a function that says whether a bit was
    flipped yet. It runs in its own process, because ArcticDB holds the GIL while it does I/O, so threads in the test
    process couldn't forward its requests."""
    receiver, sender = multiprocessing.Pipe(duplex=False)
    proxy = multiprocessing.get_context("spawn").Process(
        target=_serve, args=(target_port, pattern, sender), daemon=True
    )
    proxy.start()
    try:
        if not receiver.poll(60):
            raise RuntimeError("the byte-flipping proxy did not start")
        yield receiver.recv(), lambda: receiver.poll(5)
    finally:
        proxy.terminate()
        proxy.join()
