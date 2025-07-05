count_mobile_bank_perms = """
            SELECT
                (COUNT(*) * 5) AS mobile_bank,
                COUNT(*) AS mobile_bank_connects
            FROM mobile_bank
            WHERE surname = %(surname)s
"""


get_mobile_bank_data = """
            SELECT inn 
            FROM mobile_bank
            WHERE surname = %(surname)s
"""
