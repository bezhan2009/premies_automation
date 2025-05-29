get_worker_id_by_owner_name = """
        SELECT workers.id
        FROM workers
        JOIN users ON users.id = workers.user_id
        WHERE users.username = %(owner_name)s;
"""
