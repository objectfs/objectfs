class SingletonType(type):

    def __call__(cls, *args, **kwargs):
        """Check if class instance exits else 
        intialize a new class instance if it does not exist"""
        try:
            return cls.__instance
        except AttributeError:
            cls.__instance = super(SingletonType, cls).__call__(*args, **kwargs)
            return cls.__instance
