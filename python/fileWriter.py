import time
from typing import Callable, Optional, Union


class FileWriter:
    def __init__(
        self,
        output_path: str,
        mode: str = "wb",
        buffer_size: int = 1024 * 1024,   # 1MB
        batch_size: int = 10_000,
        encoding: str = "utf-8",
        progress_callback: Optional[Callable[[int, float], None]] = None,
    ):
        """
        :param output_path: 输出文件路径
        :param mode: 'wb' or 'w'
        :param buffer_size: 文件系统 buffer
        :param batch_size: 内部聚合条数
        :param encoding: str -> bytes 时使用
        :param progress_callback: progress(records_written, elapsed_seconds)
        """
        self.output_path = output_path
        self.mode = mode
        self.buffer_size = buffer_size
        self.batch_size = batch_size
        self.encoding = encoding
        self.progress_callback = progress_callback

        self._file = None
        self._buffer = []
        self._written_records = 0
        self._start_time = None

    # ---------- lifecycle ----------

    def __enter__(self):
        self.open()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def open(self):
        if self._file:
            return

        self._file = open(self.output_path, self.mode, buffering=self.buffer_size)
        self._start_time = time.time()

    def close(self):
        if not self._file:
            return

        self.flush()
        self._file.close()
        self._file = None

    # ---------- write ----------

    def write(self, record: Union[bytes, str]):
        """
        写入单条记录（进入内存 buffer）
        """
        if isinstance(record, str):
            record = record.encode(self.encoding)

        self._buffer.append(record)

        if len(self._buffer) >= self.batch_size:
            self._flush_buffer()

    def write_many(self, records):
        """
        写入多条
        """
        for r in records:
            self.write(r)

    def flush(self):
        """
        强制写入 buffer
        """
        self._flush_buffer()

    def _flush_buffer(self):
        if not self._buffer:
            return

        data = b"".join(self._buffer)
        self._file.write(data)
        self._written_records += len(self._buffer)
        self._buffer.clear()

        if self.progress_callback:
            elapsed = time.time() - self._start_time
            self.progress_callback(self._written_records, elapsed)

    # ---------- stats ----------

    @property
    def written_records(self) -> int:
        return self._written_records