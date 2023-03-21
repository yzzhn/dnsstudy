
class Domain:
    
    """Class to hold Domain related information"""
    def __init__(self, name, rank):
        self.name = name
        self.rank = rank
        self.message = None
        self.error = None

    def set_message(self, msg):
        """store DNS answer message"""
        self.message = msg

    def set_error(self, error):
        """set error message from query attempt"""
        self.error = error