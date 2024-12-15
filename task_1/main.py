import asyncio
import aiofiles
from pathlib import Path
import argparse
import logging

# Налаштування логування
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


async def read_folder(src_folder: Path, dist_folder: Path):
    try:
        # Перевірка, чи існує вихідна папка
        if not src_folder.exists():
            logging.error(f"Folder {src_folder} not found")
            return

        # Створюємо асинхронний список задач
        tasks = []

        # Рекурсивне сканування вихідної папки
        for entry in await asyncio.to_thread(lambda: list(src_folder.rglob("*"))):
            if entry.is_file():
                tasks.append(copy_file(entry, dist_folder))

        # Очікуємо виконання всіх задач
        await asyncio.gather(*tasks)
        logging.info("All files have been copied.")
    except Exception as e:
        logging.error(f"An error ocured while reading folder: {e}")


async def copy_file(file_path: Path, dist_folder: Path):
    try:
        # Визначення розширення файлу
        extension = file_path.suffix.lower() or "others"
        target_folder = dist_folder / extension.lstrip(".")
        await asyncio.to_thread(lambda: target_folder.mkdir(parents=True, exist_ok=True))

        target_file = target_folder / file_path.name

        # Асинхронне читання та запис файлу
        async with aiofiles.open(file_path, "rb") as source_file:
            async with aiofiles.open(target_file, "wb") as dest_file:
                while chunk := await source_file.read(1024 * 1024):  # Читаємо файл частинами
                    await dest_file.write(chunk)

        logging.info(f"File {file_path} has been copied to {target_file}")
    except Exception as e:
        logging.error(f"An error occured while copying file {file_path}: {e}")


def main():
    # Створення парсера аргументів
    parser = argparse.ArgumentParser(description="Asynchronous files copy")
    parser.add_argument("--src", type=str, required=True, help="Path to the source folder")
    parser.add_argument("--dist", type=str, required=True, help="Path to the destination folder")
    args = parser.parse_args()

    # Перетворення шляхів
    src_folder = Path(args.src).resolve()
    dist_folder = Path(args.dist).resolve()

    # Запуск асинхронної функції
    asyncio.run(read_folder(src_folder, dist_folder))


if __name__ == "__main__":
    main()
