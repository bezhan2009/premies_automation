count_workers_prem_query = """
        SELECT
            EXTRACT(MONTH FROM issue_date) AS month,
            COUNT(*) AS cards_issued,
            SUM(coast) AS prem,
            SUM(debt_osd) AS debt_osd
        FROM cards
        WHERE owner_name = %(owner_name)s
          AND EXTRACT(YEAR FROM issue_date) = %(year)s
          AND EXTRACT(MONTH FROM issue_date) = %(month)s
          AND EXTRACT(MONTH FROM expire_date) = %(month)s
          AND expire_date IS NOT NULL
        GROUP BY month
        ORDER BY month;
"""

count_workers_card_turnover_query = """
        SELECT
            SUM(COALESCE(out_balance) + COALESCE(debt_osd)) * 0.00005 AS turnover
        FROM cards
        WHERE owner_name = %(owner_name)s
          AND expire_date IS NOT NULL;
"""

count_workers_cards_activations_prem = """
        SELECT
            COUNT(*) AS cards_actived,
            (COUNT(*) * 0.8) AS cards_issued
        FROM cards
        WHERE owner_name = %(owner_name)s
          AND expire_date IS NOT NULL;
"""

count_turnovers_and_activation_cards_worker = """
        SELECT
            (SELECT
                (COUNT(*) * 0.8) AS activated_cards_prem
            FROM cards
            WHERE owner_name = %(owner_name)s
              AND expire_date IS NOT NULL
            ),
            SUM(COALESCE(out_balance) + COALESCE(debt_osd)) * 0.00005 AS turnover
        FROM cards
        WHERE owner_name = %(owner_name)s
          AND expire_date IS NOT NULL;
"""

get_cards_detail = """
        SELECT expire_date,
               issue_date,
               card_type,
               code,
               in_balance,
               debt_osd,
               debt_osk,
               out_balance,
               owner_name,
               coast
        FROM cards
        WHERE owner_name = %(owner_name)s;
"""
