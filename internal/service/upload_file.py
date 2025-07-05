import os
import time

from pkg.logger.logger import setup_logger
from pkg.errors.permission_denied_error import PermissionDeniedError
from pkg.errors.not_found_error import NotFoundError
from configs.load_configs import get_config

logger = setup_logger(__name__)


def upload_file(request) -> str:
    try:
        configs = get_config()
        original_filename = request.filename
        unix_time = int(time.time())

        # добавляем _UNIXTIME перед расширением (если есть)
        name, ext = os.path.splitext(original_filename)
        new_filename = f"{name}_{unix_time}{ext}"
        save_path = os.path.join(configs.automation_details.def_out_paths.uploaded_files_path, new_filename)

        # сохраняем файл
        with open(save_path, "wb") as f:
            f.write(request.file_content)

        logger.info(f"file saved: {save_path}")
    except Exception as e:
        logger.error("upload_file error: {}".format(e))
        raise e

    return save_path


def download_file(request) -> bytes:
    try:
        requested_path = request.path

        # Проверяем, чтобы путь был строго внутри uploads
        abs_uploads_dir = os.path.abspath("uploads")
        abs_file_path = os.path.abspath(requested_path)

        if not abs_file_path.startswith(abs_uploads_dir):
            raise PermissionDeniedError

        if not os.path.exists(abs_file_path):
            raise NotFoundError

        with open(abs_file_path, "rb") as f:
            content = f.read()

        logger.info(f"File {requested_path} has been sended successfully.")
        return content
    except PermissionDeniedError as pr:
        raise pr
    except NotFoundError as nt:
        raise nt
    except Exception as e:
        logger.error("download_file error: {}".format(e))
        raise e
