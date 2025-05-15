count_workers_prem_query = """
        SELECT
            EXTRACT(MONTH FROM issue_date) AS month,
            COUNT(*) AS cards_issued,
            SUM(coast) AS prem
        FROM cards
        WHERE owner_name = %(owner_name)s
          AND EXTRACT(YEAR FROM issue_date) = %(year)s
          AND EXTRACT(MONTH FROM issue_date) = %(month)s
          AND EXTRACT(MONTH FROM expire_date) = %(month)s
          AND debt_osd > 0
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
          AND debt_osd > 0
          AND expire_date IS NOT NULL;
"""

count_turnovers_and_activation_cards_worker = """
        SELECT
            (SELECT
                (COUNT(*) * 0.8) AS activated_cards
            FROM cards
            WHERE owner_name = %(owner_name)s
              AND debt_osd > 0
              AND expire_date IS NOT NULL
            ),
            SUM(COALESCE(out_balance) + COALESCE(debt_osd)) * 0.00005 AS turnover
        FROM cards
        WHERE owner_name = %(owner_name)s
          AND expire_date IS NOT NULL;
"""
