count_mobile_bank_perms = """
            SELECT
                (COUNT(*) * 5) AS mobile_bank
            FROM mobile_bank
            WHERE surname = %(surname)s
"""
