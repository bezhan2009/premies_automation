import hashlib
import json
import os

from cryptography.fernet import Fernet


def hash_sha256(input_string):
    # Преобразуем строку в байты (UTF-8 кодировка)
    bytes_input = input_string.encode('utf-8')
    # Создаем SHA-256 хеш
    sha256_hash = hashlib.sha256(bytes_input)
    # Возвращаем хеш в шестнадцатеричном формате
    return sha256_hash.hexdigest()


_key = None
_cipher = None


def init_encryption():
    global _key
    global _cipher

    env_key = os.getenv("KEY")
    if not env_key:
        raise RuntimeError("KEY is not set in environment variables")

    try:
        _key = env_key.encode("utf-8")
        _cipher = Fernet(_key)
    except Exception as e:
        raise ValueError(f"Invalid Fernet key: {e}")


def encrypt_any(data):
    """Сериализация, шифрование и преобразование в строку."""
    json_data = json.dumps(data)
    encrypted = _cipher.encrypt(json_data.encode("utf-8"))
    return encrypted.decode("utf-8")  # <-- добавили decode


def decrypt_any(encrypted_data):
    """Расшифровка, десериализация."""
    decrypted_bytes = _cipher.decrypt(encrypted_data)
    json_data = decrypted_bytes.decode('utf-8')
    return json.loads(json_data)
#
#
# # Пример использования:
# original_data = {
#     'name': 'ChatGPT',
#     'password': 'secret123',
#     'numbers': [1, 2, 3],
#     'nested': {'a': 1}
# }
# print("Исходные данные:", original_data)
#
# # Шифруем
# init_encryption()
# encrypted = encrypt_any("УМАРОВ ФАРИДУН")
# print("Зашифрованные данные (б bytes):", encrypted)
#
# # Расшифровываем
# decrypted = decrypt_any(encrypted)
# print("Расшифрованные данные:", decrypted)
#
# # Ключ можно сохранить или передать для последующего расшифрования
# print("Ключ для дешифровки:", _key.decode())
