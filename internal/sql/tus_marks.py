call_center_procent = """
            SELECT
                SUM(mark)::float / COUNT(*) AS callcenter
            FROM tus_marks
            WHERE tus_code = %(owner_name)s
              AND EXTRACT(YEAR FROM req_date) = %(year)s
              AND EXTRACT(MONTH FROM req_date) = %(month)s;
"""
