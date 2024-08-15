"""Module containing classes used to handle errors"""


class AuthError(Exception):
    """
    A custom exception class for handling authentication errors.

    Attributes:
        error_msg (str): The error message to be displayed when the exception is raised.
        status_code (int): The HTTP status code associated with the error.

    Args:
        error_msg (str): The error message to be displayed when the exception is raised.
        status_code (int): The HTTP status code associated with the error.
    """

    def __init__(self, error_msg: str, status_code: int):
        super().__init__(error_msg)

        self.error_msg = error_msg
        self.status_code = status_code
