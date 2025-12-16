import time

from Generator import DataCreator
from fileWriter import FileWriter


def progress(records, elapsed):
    speed = records / elapsed if elapsed > 0 else 0
    print(
        f"\r已写入 {records:,} 条 "
        f"{speed:,.0f} 条/s",
        end="",
        flush=True,
    )


def main():
    target_records = 200_000
    record_size = 256  # 约 5GB
    batch_size = 20_000

    creator = DataCreator(
        output_path="ignored.txt",  # 示例中由 FileWriter 接管输出
        total_records=target_records,
        record_size=record_size,
        batch_size=batch_size,
        show_progress=False,
    )

    start = time.time()
    with FileWriter(
        output_path="test_data_5gb.txt",
        batch_size=batch_size,
        progress_callback=progress,
    ) as writer:
        for batch in creator.iterate_batches():
            writer.write_many(batch)

    elapsed = time.time() - start
    print(f"\n写入完成，用时 {elapsed:.2f}s")


if __name__ == "__main__":
    main()