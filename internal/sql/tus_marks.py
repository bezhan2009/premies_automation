call_center_procent = """
            SELECT
                SUM(mark)::float / COUNT(*) AS callcenter
            FROM tus_marks
            WHERE surname = %(owner_name)s
"""

call_center_tests_and_complaints = """
            SELECT
                tests,
                complaints
            FROM tus_marks
            WHERE surname = %(owner_name)s
"""
