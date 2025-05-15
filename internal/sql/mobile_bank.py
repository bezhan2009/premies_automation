count_mobile_bank_perms = """
            SELECT
                (COUNT(*) * 5) AS mobile_bank
            FROM mobile_bank
            WHERE surname = %(surname)s 
              # AND EXTRACT(YEAR FROM req_date) = %(year)s
              # AND EXTRACT(MONTH FROM req_date) = %(month)s;
"""
