"""organize_media.py – Скрипт для сортировки и дедупликации файлов с логированием.

Использование:
    python organize_media.py <SOURCE_DIR> [<DEST_DIR>]

• Обходит <SOURCE_DIR> рекурсивно и распределяет файлы по категориям
  Фото, Видео, Музыка, Программы, Прочее во <DEST_DIR> (по умолчанию –
  папка «sorted_media» рядом с исходной).
• Для всех файлов вычисляет SHA‑256 и копирует только
  уникальные файлы.
• Фотографии и видео переименовываются
      YYYYMMDD_HHMMSS_старое_имя.ext (пробелы → _)
  Дата берётся из EXIF/mtime.
• Аудио‑файлы и программы копируются с исходным именем.
• Логирование ключевых этапов работы.

Требования:
    pip install pillow

Пример:
    python organize_media.py /home/user/Downloads
"""
from __future__ import annotations

import argparse
import hashlib
import logging
import os
import re
import shutil
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Dict, Set

try:
    from PIL import Image, ExifTags
except ImportError:
    Image = None  # type: ignore

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

PHOTO_EXT = {
    ".jpg",
    ".jpeg",
    ".png",
    ".gif",
    ".bmp",
    ".thm",
    ".tiff",
    ".heic",
    ".heif",
    ".webp",
    ".arw",
    ".nef",
    ".cr2",
}
VIDEO_EXT = {
    ".mp4",
    ".mov",
    ".avi",
    ".mkv",
    ".wmv",
    ".flv",
    ".webm",
    ".mpeg",
    ".mpg",
    ".m4v",
}
MUSIC_EXT = {".mp3", ".aac", ".flac", ".ogg", ".wav", ".m4a", ".wma"}
PROGRAM_EXT = {
    ".exe",
    ".msi",
    ".apk",
    ".deb",
    ".rpm",
    ".dmg",
    ".pkg",
    ".app",
    ".bat",
    ".sh",
    ".ps1",
    ".jar",
}

CATEGORY_DIR = {
    "photo": "Фото",
    "video": "Видео",
    "music": "Музыка",
    "program": "Программы",
    "other": "Прочее",
}

CHUNK_SIZE = 1 << 20  # 1 MiB


def compute_sha256(path: Path) -> str:
    """Вычислить SHA‑256 хеш файла без загрузки в память целиком."""
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(CHUNK_SIZE), b""):
            h.update(chunk)
    return h.hexdigest()


def get_photo_datetime(path: Path) -> datetime | None:
    """Попытаться извлечь дату из EXIF, иначе None."""
    if Image is None:
        return None
    try:
        with Image.open(path) as img:
            exif = img._getexif()
            if not exif:
                return None
            exif_data = {ExifTags.TAGS.get(k, k): v for k, v in exif.items()}
            dt_bytes = exif_data.get("DateTimeOriginal") or exif_data.get("DateTime")
            if dt_bytes:
                if isinstance(dt_bytes, bytes):
                    dt_bytes = dt_bytes.decode(errors="ignore")
                try:
                    return datetime.strptime(dt_bytes, "%Y:%m:%d %H:%M:%S")
                except ValueError:
                    return None
    except Exception as e:
        logging.warning(f"Не удалось получить EXIF у {path}: {e}")
    return None


def sanitized_name(name: str) -> str:
    """Заменяем пробелы на нижнее подчёркивание и убираем лишние подчёркивания."""
    name = re.sub(r"\s+", "_", name.strip())
    name = re.sub(r"_+", "_", name)
    return name


def unique_path(dest_dir: Path, filename: str) -> Path:
    """Вернуть несуществующий путь, добавляя счётчик при необходимости."""
    target = dest_dir / filename
    counter = 1
    stem, ext = target.stem, target.suffix
    while target.exists():
        target = dest_dir / f"{stem}_{counter}{ext}"
        counter += 1
    return target


def categorize(path: Path) -> str:
    ext = path.suffix.lower()
    if ext in PHOTO_EXT:
        return "photo"
    if ext in VIDEO_EXT:
        return "video"
    if ext in MUSIC_EXT:
        return "music"
    if ext in PROGRAM_EXT:
        return "program"
    return "other"


def organise(source: Path, dest: Path) -> None:
    logging.info(f"Начало сортировки: {source} -> {dest}")
    dest.mkdir(exist_ok=True)
    for key, dir_name in CATEGORY_DIR.items():
        subdir = dest / dir_name
        subdir.mkdir(exist_ok=True)
        logging.info(f"Категория '{key}' создаётся в {subdir}")

    seen_hashes: Dict[str, Set[str]] = defaultdict(set)

    for root, _, files in os.walk(source):
        for file in files:
            src_path = Path(root) / file
            category = categorize(src_path)
            dest_subdir = dest / CATEGORY_DIR[category]
            logging.info(f"Обработка файла {src_path} как {category}")

            file_hash = compute_sha256(src_path)
            if file_hash in seen_hashes[category]:
                logging.info(f"Пропуск дубликата {src_path}")
                continue
            seen_hashes[category].add(file_hash)

            if category in {"photo", "video"}:
                dt = get_photo_datetime(src_path) if category == "photo" else None
                if dt is None:
                    dt = datetime.fromtimestamp(src_path.stat().st_mtime)
                date_part = dt.strftime("%Y%m%d_%H%M%S")
                old_stem = sanitized_name(src_path.stem)
                new_name = f"{date_part}_{old_stem}{src_path.suffix.lower()}"
            elif category == "music":
                new_name = src_path.name
            else:
                new_name = src_path.name

            new_path = unique_path(dest_subdir, new_name)
            try:
                if not new_path.exists():
                    shutil.copy2(src_path, new_path)
                    logging.info(f"Скопировано: {src_path} -> {new_path}")
            except Exception as e:
                logging.error(f"Не удалось копировать {src_path}: {e}")

    logging.info("Сортировка завершена")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Сортировка фото, видео, музыки, программ и прочего с логированием."
    )
    parser.add_argument("source", type=Path, help="Исходная директория")
    parser.add_argument(
        "dest",
        nargs="?",
        type=Path,
        default=None,
        help="Директория назначения (по умолчанию 'sorted_media' рядом с исходной)",
    )
    args = parser.parse_args()

    source_dir = args.source.resolve()
    dest_dir = args.dest.resolve() if args.dest else source_dir.parent / "sorted_media"

    if not source_dir.exists():
        logging.error(f"Директория {source_dir} не существует")
        raise SystemExit(1)

    organise(source_dir, dest_dir)
