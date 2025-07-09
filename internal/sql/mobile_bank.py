count_mobile_bank_perms = """
            SELECT
                (COUNT(*) * 10) AS mobile_bank,
                mobile_bank_connects
            FROM mobile_bank
            WHERE surname = %(surname)s group by mobile_bank_connects
"""


get_mobile_bank_data = """
            SELECT mobile_bank_connects 
            FROM mobile_bank
            WHERE surname = %(surname)s
"""
