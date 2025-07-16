count_workers_prem_query = """
        SELECT
            COUNT(*) AS cards_issued,
            SUM(coast_cards) AS prem_cards,
            SUM(coast_credits) AS prem_credits,
            SUM(debt_osd) AS debt_osd,
            SUM(debt_osk) AS debt_osk,
            SUM(in_balance) AS in_balance,
            SUM(out_balance) AS out_balance
        FROM card_details
        WHERE similarity(owner_name, %(owner_name)s) > 0.3
          AND issue_date >= DATE '2022-01-01'
          AND issue_date < make_date(%(year)s, %(month)s, 1) + interval '1 month';
"""

count_workers_prem_query_dates = """
        SELECT
            COUNT(*) AS cards_issued,
            SUM(coast_cards) AS prem_cards,
            SUM(coast_credits) AS prem_credits,
            SUM(debt_osd) AS debt_osd,
            SUM(debt_osk) AS debt_osk,
            SUM(in_balance) AS in_balance,
            SUM(out_balance) AS out_balance
        FROM card_details
        WHERE similarity(owner_name, %(owner_name)s) > 0.3
          AND EXTRACT(YEAR FROM issue_date) = %(year)s
          AND EXTRACT(MONTH FROM issue_date) = %(month)s
"""

count_workers_cards_sailed = """
        SELECT
            COUNT(*) AS cards_issued
        FROM card_details
        WHERE similarity(owner_name, %(owner_name)s) > 0.3
          AND EXTRACT(YEAR FROM issue_date) = %(year)s
          AND EXTRACT(MONTH FROM issue_date) = %(month)s
"""

count_workers_cards_sailed_in_general = """
        SELECT
            COUNT(*) AS cards_issued
        FROM card_details
        WHERE similarity(owner_name, %(owner_name)s) > 0.3
          AND issue_date >= DATE '2022-01-01'
          AND issue_date < make_date(%(year)s, %(month)s, 1) + interval '1 month';
"""

count_workers_card_turnover_query = """
        SELECT
            SUM(COALESCE(out_balance) + COALESCE(debt_osd)) * 0.00005 AS turnover
        FROM card_details
        WHERE similarity(owner_name, %(owner_name)s) > 0.3
          AND expire_date IS NOT NULL;
"""

count_workers_cards_activations_prem = """
        SELECT
            COUNT(*) AS cards_actived,
            (COUNT(*) * 0.8) AS cards_prems
        FROM card_details
        WHERE similarity(owner_name, %(owner_name)s) > 0.3
          AND debt_osd > 0
          AND expire_date IS NOT NULL;
"""

count_turnovers_and_activation_cards_worker = """
        SELECT
            COUNT(*) * 0.8 AS activated_cards_prem,
            COUNT(*) AS activated_cards,
            SUM(COALESCE(out_balance, 0) + COALESCE(debt_osd, 0)) * 0.00005 AS turnover
        FROM card_details
        WHERE similarity(owner_name, %(owner_name)s) > 0.3
          AND debt_osd > 0
          AND expire_date IS NOT NULL;
"""

count_turnovers_and_activation_cards_worker_credit = """
        SELECT
            COUNT(*) * 0.8 AS activated_cards_prem,
            COUNT(*) AS activated_cards,
            SUM(COALESCE(out_balance, 0) + COALESCE(debt_osd, 0)) * 0.0001 AS turnover
        FROM card_details
        WHERE similarity(owner_name, %(owner_name)s) > 0.3
          AND debt_osd > 0
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
               coast_cards,
               coast_credits
        FROM card_details
        WHERE similarity(owner_name, %(owner_name)s) > 0.3;
"""
