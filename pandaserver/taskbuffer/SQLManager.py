class SQLQueryManager:
    def __init__(self, sql_query):
        self.sql_query = sql_query

    def format_query(self):
        """Format the SQL query by removing line breaks and redundant spaces."""
        # Replace newline characters with a single space
        formatted_query = self.sql_query.replace('\n', ' ')
        # Remove redundant spaces (more than one space in a row)
        formatted_query = ' '.join(formatted_query.split())
        return formatted_query

    def set_query(self, sql_query):
        """Set a new SQL query."""
        self.sql_query = sql_query

    def get_query(self):
        """Get the current SQL query."""
        return self.sql_query

    def log_query(self):
        """Log the formatted SQL query."""
        formatted_query = self.format_query()
        print(f"SQL Query: {formatted_query}")

    def append(self, sql_text):
        """Append more text to the existing SQL query."""
        self.sql_query += ' ' + sql_text

    def __str__(self):
        """Custom string representation of the SQLQueryManager instance."""
        return self.format_query()


# Example usage:
if __name__ == "__main__":
    # Initialize the SQLQueryManager with an SQL query
    sql_manager = SQLQueryManager("""
        SELECT *
        FROM your_table
        WHERE condition = 'something'
        ORDER BY column_name
    """)

    # Log the SQL query
    print(sql_manager)  # This will print the formatted SQL query

    # Change the SQL query
    new_query = """
        SELECT id, name
        FROM another_table
        WHERE condition = 'another condition'
    """
    sql_manager.set_query(new_query)
    sql_manager.append("""
    AND condition2 = 'more conditions'
    """)

    # Log the updated SQL query
    print(sql_manager)  # This will print the updated formatted SQL query
