from typing import List

from psycopg2 import sql

from internal.app.models.application import ApplicationInfo
from pkg.db.connect import get_connection
from pkg.logger.logger import setup_logger

logger = setup_logger(__name__)


def get_application_by_ids(application_ids: List[int]) -> List[ApplicationInfo]:
    if not application_ids:
        return []

    # Безопасная вставка значений в IN
    placeholders = sql.SQL(',').join(sql.Placeholder() * len(application_ids))
    query = sql.SQL("""
        SELECT id, 
            tax_workers_code, 
            name, 
            surname, 
            patronymic, 
            gender, 
            client_index, 
            issued_by, 
            phone_number, 
            secret_word, 
            card_name, 
            card_code, 
            front_side_of_the_passport, 
            back_side_of_the_passport, 
            selfie_with_passport, 
            inn, 
            delivery_address, 
            country, 
            region, 
            city, 
            district, 
            street_type, 
            house_number, 
            corpus, 
            apartment_number, 
            document_number, 
            passport_issued_at, 
            receiving_office, 
            clients_code, 
            type_of_certificate, 
            documents_series, 
            is_resident, 
            embossed_name, 
            application_status_id, 
            birth_date, 
            issued_at,
            population_type,
            populated,
            street
        FROM applications
        WHERE id IN ({})
    """).format(placeholders)

    try:
        conn = get_connection()
        with conn.cursor() as cursor:
            cursor.execute(query, application_ids)
            rows = cursor.fetchall()

        applications = []
        for row in rows:
            applications.append(
                ApplicationInfo(
                    id=row[0],
                    tax_workers_code=row[1],
                    name=row[2],
                    surname=row[3],
                    patronymic=row[4],
                    gender=row[5],
                    client_index=row[6],
                    issued_by=row[7],
                    phone_number=row[8],
                    secret_word=row[9],
                    card_name=row[10],
                    card_code=row[11],
                    front_side_of_the_passport=row[12],
                    back_side_of_the_passport=row[13],
                    selfie_with_passport=row[14],
                    inn=row[15],
                    delivery_address=row[16],
                    country=row[17],
                    region=row[18],
                    city=row[19],
                    district=row[20],
                    street_type=row[21],
                    house_number=row[22],
                    corpus=row[23],
                    apartment_number=row[24],
                    document_number=row[25],
                    passport_issued_at=row[26],
                    receiving_office=row[27],
                    clients_code=row[28],
                    type_of_certificate=row[29],
                    documents_series=row[30],
                    is_resident=row[31],
                    embossed_name=row[32],
                    application_status_id=row[33],
                    birth_date=row[34],
                    issued_at=row[35],
                    population_type=row[36],
                    populated=row[37],
                    street=row[38]
                )
            )

        return applications

    except Exception as e:
        logger.error("Failed to fetch applications: %s", e)
        raise e
