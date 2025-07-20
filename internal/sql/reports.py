count_user_cards_category_issued = """
        SELECT
            cp.category,
            COUNT(*) AS cards_count
        FROM card_details c
        JOIN card_prices cp ON c.card_type = cp.dcl_name
        WHERE similarity(owner_name, %(owner_name)s) > 0.3
          AND EXTRACT(YEAR FROM c.issue_date) = %(year)s
          AND EXTRACT(MONTH FROM c.issue_date) = %(month)s
        GROUP BY cp.category
        ORDER BY cards_count DESC;
"""

count_user_cards_turnover_out_balance_debt_osd = """
        SELECT
            SUM(debt_osd) AS debt_osd,
            SUM(out_balance) AS out_balance,
            SUM(COALESCE(out_balance) + COALESCE(debt_osd)) * 0.00005 AS turnover_cards,
            SUM(COALESCE(out_balance) + COALESCE(debt_osd)) * 0.0001 AS turnover_credits
        FROM card_details
        WHERE similarity(owner_name, %(owner_name)s) > 0.3
          AND expire_date IS NOT NULL;
"""

count_service_qualities_balls = """
        SELECT 
                complaint, 
                tests, 
                call_center
        FROM service_qualities
        WHERE worker_id = %(owner_id)s
          AND EXTRACT(YEAR FROM created_at) = %(year)s
          AND EXTRACT(MONTH FROM created_at) = %(month)s;
"""

count_mobile_bank_perms = """
        SELECT mobile_bank_connects
        FROM mobile_bank_sales
        WHERE worker_id = %(owner_id)s
          AND EXTRACT(YEAR FROM created_at) = %(year)s
          AND EXTRACT(MONTH FROM created_at) = %(month)s;
"""

count_overdraft_perm = """
        SELECT overdraft_perm
        FROM overdrafts
        WHERE worker_id = %(owner_id)s
          AND EXTRACT(YEAR FROM created_at) = %(year)s
          AND EXTRACT(MONTH FROM created_at) = %(month)s;
"""

get_worker_data = """
        SELECT id, position, salary, salary_project, plan, place_work
        FROM workers
        WHERE user_id = %(owner_id)s;
"""

get_worker_place_data = """
        SELECT o.title, o.description
        FROM offices o
        JOIN office_users ou ON o.id = ou.office_id
        WHERE ou.worker_id = %(owner_id)s;
"""

count_activated_card_perms = """
        SELECT activated_cards
        FROM card_turnovers
        WHERE worker_id = %(owner_id)s;
"""


count_activated_card_sales = """
        SELECT active_cards_perms
        FROM card_turnovers
        WHERE worker_id = %(owner_id)s;
"""

get_role_id_by_worker_id = """
        SELECT u.role_id
        FROM users u
        JOIN workers w ON u.id = w.user_id
        WHERE w.id = %(owner_id)s;
"""
