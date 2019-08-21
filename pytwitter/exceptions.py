class SpiderException(Exception):

    def __init__(self, value):
        self.value = value

    def __str__(self):
        return self.value


class NetworkException(SpiderException):

    def __init__(self, value):
        super(NetworkException, self).__init__(value)
        self.msg = 'Network Error'


class TooManyErrorsException(SpiderException):

    def __init__(self, value):
        super(TooManyErrorsException, self).__init__(value)
        self.msg = 'Too Many Errors'
