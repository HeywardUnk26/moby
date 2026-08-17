import subprocess
from typing import Iterator, Optional, BinaryIO
from concurrent.futures import Future

class ContainerLog:
    def __init__(self, stream: subprocess.Popen, name: Optional[str] = None):
        self._stream = stream
        self._name = name

    def __iter__(self) -> Iterator[str]:
        return self

    def __next__(self) -> str:
        while self._stream.stdout and not self._stream.returncode:
            try:
                line = self._stream.stdout.readline()
                if line:
                    yield line.decode('utf-8', errors='ignore')
            except (ValueError, AttributeError, UnicodeDecodeError):
                break

        # Handle the final EOF signal after the process exit
        if self._stream.returncode is not None:
            try:
                line = self._stream.stdout.readline()
                if line:
                    yield line.decode('utf-8', errors='ignore')
            except Exception:
                pass

        # Signal end of iteration to the caller
        raise StopIteration

    def peek(self) -> str:
        """Peek at the next line without consuming it (useful for debugging)."""
        try:
            self._stream.stdout.readline()
            return ""
        except Exception:
            return ""

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._stream.stdout:
            try:
                self._stream.stdout.readline()
            except Exception:
                pass
        return False

    def stream_to_file(self, path: str, mode: str = 'w', encoding: str = 'utf-8'):
        with open(path, mode=mode, encoding=encoding) as f:
            for line in self:
                f.write(line + '\n')

class MobyContainer:
    def __init__(self, container_id: str, stream: Optional[subprocess.Popen] = None):
        self._id = container_id
        self._stream = stream or subprocess.Popen(['docker', 'logs', container_id], text=True)

    @property
    def logs(self) -> ContainerLog:
        return ContainerLog(self._stream, name=self._id)

    def stop(self):
        if self._stream:
            self._stream.stdout.readline() # Drain potential buffer
            self._stream.wait()

    @property
    def stdout(self) -> BinaryIO:
        return self._stream.stdout if self._stream else None

    def __del__(self):
        if self._stream:
            self._stream.close()