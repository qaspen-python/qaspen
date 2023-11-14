def parse_database(database_url: str) -> str:
    """Parse database from connection url.

    ### Returns:
    Connection from connection url.
    """
    return database_url.split("/")[-1]
