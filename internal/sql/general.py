get_worker_id_by_owner_name = """
        SELECT workers.id, role_id
        FROM workers
        JOIN users ON users.id = workers.user_id
        WHERE REPLACE(users.full_name, ' ', '') = REPLACE(%(owner_name)s, ' ', '');
"""

get_card_by_code = """
    SELECT id
    FROM card_details
    WHERE code = %(card_code)s
"""
