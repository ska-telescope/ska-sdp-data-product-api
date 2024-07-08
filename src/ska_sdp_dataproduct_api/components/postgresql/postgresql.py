"""Module adds a PostgreSQL interface for persistent storage of metadata files"""

import psycopg

from ska_sdp_dataproduct_api.configuration.settings import (
    POSTGRESQL_HOST,
    POSTGRESQL_PASSWORD,
    POSTGRESQL_PORT,
    POSTGRESQL_USER,
)


class PostgresConnector:  # pylint: disable=too-many-instance-attributes
    """
    A class to connect to a PostgreSQL instance and test its availability.
    """

    def __init__(self):
        self.host: str = POSTGRESQL_HOST
        self.port: int = POSTGRESQL_PORT
        self.user: str = POSTGRESQL_USER
        self.password: str = POSTGRESQL_PASSWORD
        self.conn = None
        self.postgresql_running: bool = False
        self.postgresql_version: str = ""
        self.connection_established_at = ""
        self.connection_error = ""

        self.connect()

    def status(self) -> dict:
        """
        Retrieves the current status of the PostgreSQL connection.

        This method returns a dictionary containing the following information:

        * `host`: The hostname or IP address of the PostgreSQL instance.
        * `port`: The port number on which PostgreSQL is listening.
        * `database`: The name of the database being connected to.
        * `user`: The username used for authentication with PostgreSQL.
        * `running` (optional): A boolean indicating whether PostgreSQL is currently running
        (if available).
        * `postgresql_version` (optional): The version of PostgreSQL being used (if applicable).
        * `connection_established_at` (optional): A timestamp representing when a connection was
        last established with PostgreSQL. This value might not be available depending on the
        database library used.
        * `connection_error` (optional): An error object containing details about any connection
        errors that occurred (if applicable).

        Returns:
            A dictionary containing the current status information.
        """

        return {
            "host": self.host,
            "port": self.port,
            "user": self.user,
            "running": self.postgresql_running,
            "postgresql_version": self.postgresql_version,  # Optional
            "connection_error": self.connection_error,  # Optional
        }

    def connect(self) -> None:
        """
        Attempts to connect to the PostgreSQL instance.

        Returns:
          None.
        """
        try:
            self.conn = psycopg.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
            )
            self.postgresql_running = True

            # Attempt to retrieve PostgreSQL version if connected
            with self.conn.cursor() as cursor:
                cursor.execute("SELECT version()")
                self.postgresql_version = cursor.fetchone()[0]

        except (Exception, psycopg.Error) as error:
            self.conn = None
            self.postgresql_running = False
            raise error  # Re-raise the error for handling

    def disconnect(self) -> None:
        """
        Closes the connection to the PostgreSQL instance if it's open.
        """
        if self.conn:
            self.conn.close()
            self.conn = None

    def is_running(self):
        """
        Tests if the PostgreSQL instance is running by attempting a connection.

        Returns:
          True if connection is successful, False otherwise.
        """
        if self.conn is None:
            # If no existing connection, try creating a new one
            return self.connect()
        # Existing connection, check if it's still valid (using a simple query)
        try:
            cursor = self.conn.cursor()
            cursor.execute("SELECT 1")
            cursor.close()
            return True
        except (Exception, psycopg.Error) as error:  # pylint: disable=broad-exception-caught
            print("Connection error, likely PostgreSQL instance is down:", error)
            self.conn.close()
            self.conn = None
            return False

    def close(self):
        """
        Closes the connection to the PostgreSQL instance if open.
        """
        if self.conn:
            self.conn.close()
            self.conn = None
