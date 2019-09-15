class SpiderException(Exception):

    def __init__(self, value=''):
        self.value = value

    def __str__(self):
        return self.value


class NetworkException(SpiderException):

    def __init__(self, value=''):
        super(NetworkException, self).__init__(value)
        self.msg = 'Network Error'


class RetryLimitExceededException(SpiderException):

    def __init__(self, value=''):
        super(RetryLimitExceededException, self).__init__(value)
        self.msg = 'Retry Limit Exceeded'


class UnauthorizedException(SpiderException):

    def __init__(self, value=''):
        super(UnauthorizedException, self).__init__(value)
        self.msg = 'Unauthorized request'
