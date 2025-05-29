import os
import zipfile


def archive_directory(directory, zip_filename):
    """Archive the specified directory into a ZIP file."""
    with zipfile.ZipFile(zip_filename, 'w', zipfile.ZIP_DEFLATED) as zipf:
        for root, _, files in os.walk(directory):
            for file in files:
                file_path = os.path.join(root, file)
                zipf.write(file_path, arcname=file)
