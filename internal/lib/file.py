import os
import re
import shutil
import time

from openpyxl.cell import MergedCell

from pkg.logger.logger import setup_logger

logger = setup_logger(__name__)


def sanitize_filename(name):
    """Sanitize filename by replacing invalid characters."""
    return re.sub(r'[\\/*?:"<>|]', '_', name).replace(' ', '_')


def get_writable_cell_ref(ws, cell_ref):
    """Get the writable cell reference (top-left of merged range if applicable)."""
    cell = ws[cell_ref]
    if isinstance(cell, MergedCell):
        for merged_range in ws.merged_cells.ranges:
            if cell.coordinate in merged_range:
                top_left_cell = ws.cell(merged_range.min_row, merged_range.min_col)
                return top_left_cell.coordinate
        raise ValueError(f"MergedCell {cell_ref} not found in any merged range")
    return cell_ref


def generate_filename(employee_name):
    timestamp = int(time.time())
    sanitized_name = sanitize_filename(employee_name)
    filename = f"{sanitized_name}_{timestamp}"
    return filename


def move_and_rename_file(target_folder, filename) -> str:
    try:
        # Полный путь исходного файла (предполагаем, что он в текущей папке)
        source_path = os.path.abspath(filename)

        # Проверка существования исходного файла
        if not os.path.isfile(source_path):
            raise FileNotFoundError(f"Файл {filename} не найден в текущей директории.")

        # Создаём папку, если её нет
        if not os.path.exists(target_folder):
            os.makedirs(target_folder)

        # Получение расширения файла
        _, ext = os.path.splitext(filename)

        # Текущие UNIX-тайм
        timestamp = str(int(time.time()))

        # Формируем новое имя файла
        new_filename = f"{os.path.splitext(filename)[0]}_{timestamp}{ext}"

        # Полный путь назначения
        destination_path = os.path.join(target_folder, new_filename)

        # Перемещаем файл
        shutil.move(source_path, destination_path)
    except Exception as e:
        raise e

    # Возвращаем путь в формате 'папка/имя_файла_UNIXвремя.расширение'
    return os.path.join(target_folder, new_filename)


def clear_folder(folder_path):
    if not os.path.exists(folder_path):
        logger.error(f"folder not found: {folder_path}")
        raise FileNotFoundError(f"Папка {folder_path} не найдена.")

    for filename in os.listdir(folder_path):
        file_path = os.path.join(folder_path, filename)
        try:
            if os.path.isfile(file_path) or os.path.islink(file_path):
                logger.info(f"Deleting file: {file_path}")
                os.unlink(file_path)
            elif os.path.isdir(file_path):
                logger.info(f"Deleting file: {file_path}")
                shutil.rmtree(file_path)
        except Exception as e:
            logger.error(f"Cannot delete the file {file_path}: {e}")

    logger.info(f"cleaned folder {folder_path}")
