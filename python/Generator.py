import time

class DataCreator:
    def __init__(
        self,
        output_path: str,
        total_records: int,
        record_size: int = 500,
        batch_size: int = 10_000,
        encoding: str = "utf-8",
        show_progress: bool = True,
    ):
        self.output_path = output_path
        self.total_records = total_records
        self.record_size = record_size
        self.batch_size = batch_size
        self.encoding = encoding
        self.show_progress = show_progress

    def _generate_record(self, index: int) -> bytes:
        """
        生成一条固定大小的记录（bytes）
        """
        prefix = f"{index},TEST_DATA,".encode(self.encoding)
        padding_size = self.record_size - len(prefix) - 1  # -1 for '\n'
        if padding_size < 0:
            raise ValueError("record_size 太小，无法容纳基础字段")

        return prefix + b"x" * padding_size + b"\n"

    def generate_record(self, index: int) -> bytes:
        """对外暴露的记录生成接口，用于按需构造单条记录"""
        return self._generate_record(index)

    def iterate_batches(self):
        """按 batch_size 逐批生成记录，避免重复分配列表"""
        batch = []
        for i in range(self.total_records):
            batch.append(self._generate_record(i))

            if len(batch) >= self.batch_size:
                yield batch
                batch = []

        if batch:
            yield batch

    def create(self):
        start_time = time.time()
        written = 0

        with open(self.output_path, "wb", buffering=1024 * 1024) as f:
            buffer = []

            for i in range(self.total_records):
                buffer.append(self._generate_record(i))

                if len(buffer) >= self.batch_size:
                    f.write(b"".join(buffer))
                    written += len(buffer)
                    buffer.clear()

                    if self.show_progress:
                        self._print_progress(written, start_time)

            if buffer:
                f.write(b"".join(buffer))
                written += len(buffer)
                buffer.clear()

        if self.show_progress:
            elapsed = time.time() - start_time
            print(f"\n完成：{written} 条，用时 {elapsed:.2f}s")

    def _print_progress(self, written: int, start_time: float):
        percent = written / self.total_records * 100
        elapsed = time.time() - start_time
        speed = written / elapsed if elapsed > 0 else 0
        print(
            f"\r进度: {percent:.2f}% "
            f"({written}/{self.total_records}) "
            f"{speed:,.0f} 条/s",
            end="",
            flush=True,
        )