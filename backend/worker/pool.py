import argparse
import logging
import multiprocessing as mp
import signal
import sys
import threading
import time
from typing import TYPE_CHECKING, Any, Optional

from backend.logging_utils import configure_logging_env

from .process import WorkerProcess

configure_logging_env()

if TYPE_CHECKING:
    from multiprocessing.connection import Connection


class WorkerPool:
    def __init__(self, num_workers: int = 2):
        self.num_workers = num_workers
        self.processes: list[Optional[mp.Process]] = [None] * self.num_workers
        self.heartbeat_conns: list[Optional[Connection]] = [None] * self.num_workers
        self._shutdown = threading.Event()

    def start(self) -> None:
        self._start_all_workers()
        self._start_heartbeat_monitor()

    def _start_worker(self, i: int) -> None:
        if self._shutdown.is_set():
            logging.info(f"[Pool] Shutdown in progress. Not restarting worker {i}")
            return

        # Clean up old process
        old_proc = self.processes[i]
        if old_proc is not None:
            logging.info(f"Cleaning up old process: {old_proc.pid}")
            if old_proc.is_alive():
                old_proc.terminate()
            old_proc.join(timeout=1)
            if old_proc.is_alive():
                old_proc.kill()
                old_proc.join()

        # Clean up old heartbeat pipe
        old_conn = self.heartbeat_conns[i]
        if old_conn:
            try:
                old_conn.close()
            except Exception:
                pass

        if self._shutdown.is_set():
            logging.info(f"[Pool] Shutdown in progress. Skipping worker {i} restart")
            return

        # Start new worker
        parent_conn, child_conn = mp.Pipe(duplex=True)
        p = WorkerProcess(child_conn, name=f"worker-{i}")
        p.daemon = False
        p.start()

        if not p.is_alive():
            logging.exception(f"Worker {i} failed to start")

        self.processes[i] = p
        self.heartbeat_conns[i] = parent_conn
        logging.info(f"[Pool] Started worker {i} with PID {p.pid}")

    def _start_all_workers(self) -> None:
        for i in range(self.num_workers):
            self._start_worker(i)

    def _start_heartbeat_monitor(self) -> None:
        def monitor():
            while not self._shutdown.is_set():
                for i in range(self.num_workers):
                    if self._shutdown.is_set():
                        break  # Check again inside loop to exit early

                    conn = self.heartbeat_conns[i]
                    p = self.processes[i]

                    dead = False
                    if p is None:
                        dead = True
                    elif not p.is_alive() or p.exitcode is not None:
                        dead = True
                    elif conn is None or not conn.poll(0.5):
                        dead = True

                    if dead and not self._shutdown.is_set():
                        logging.info(
                            f"[Pool] Worker {i} is dead or unresponsive. Restarting..."
                        )
                        self._start_worker(i)

                time.sleep(1)

        threading.Thread(target=monitor, daemon=True).start()

    def shutdown(self) -> None:
        self._shutdown.set()

        # 🔁 Tell each worker to shut down via the pipe
        for i, (p, conn) in enumerate(zip(self.processes, self.heartbeat_conns)):
            if p is None:
                continue

            try:
                if conn is not None:
                    conn.send("shutdown")  # 🛑 Graceful shutdown message
            except Exception as e:
                logging.warning(f"[Pool] Failed to send shutdown to worker {i}: {e}")

        # 🔁 Join all processes
        for i, p in enumerate(self.processes):
            if p is None:
                continue
            logging.info(f"[Pool] Joining process: {p.pid}")
            p.join(timeout=2)
            if p.is_alive():
                logging.warning(
                    f"[Pool] Process {p.pid} did not exit in time, terminating..."
                )
                p.terminate()
                p.join()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("num_workers", type=int, help="Number of worker processes")

    args: argparse.Namespace = parser.parse_args()
    logging.info(f"Starting with {args.num_workers} workers")

    pool = WorkerPool(num_workers=args.num_workers)
    pool.start()

    def handle_sigterm(sig: int, frame: Any) -> None:
        pool.shutdown()
        sys.exit(0)

    signal.signal(signal.SIGINT, handle_sigterm)
    signal.signal(signal.SIGTERM, handle_sigterm)

    # Wait for signal in main thread
    threading.Event().wait()


if __name__ == "__main__":
    main()
