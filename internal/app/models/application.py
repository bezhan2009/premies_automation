from dataclasses import dataclass
from datetime import date


@dataclass
class ApplicationInfo:
    id: int
    tax_workers_code: str
    name: str
    surname: str
    patronymic: str
    gender: str
    client_index: str
    issued_by: str
    phone_number: str
    secret_word: str
    card_name: str
    card_code: str
    front_side_of_the_passport: str
    back_side_of_the_passport: str
    selfie_with_passport: str
    inn: str
    delivery_address: str
    country: str
    region: str
    city: str
    district: str
    street_type: str
    house_number: str
    corpus: str
    apartment_number: str
    document_number: str
    passport_issued_at: date
    receiving_office: str
    clients_code: str
    type_of_certificate: str
    documents_series: str
    is_resident: bool
    embossed_name: str
    application_status_id: int
    birth_date: date
    issued_at: date
    population_type: str
    populated: str
    street: str

