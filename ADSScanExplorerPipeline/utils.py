import uuid

def is_valid_uuid(value):
    """Utility function to test if a string is a valid uuid"""
    try:
        uuid.UUID(value)
        return True
    except ValueError:
        return False