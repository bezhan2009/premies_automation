call_center_procent = """
            SELECT
                SUM(mark)::float / COUNT(*) AS callcenter
            FROM tus_marks
            WHERE surname = %(owner_name)s
"""
