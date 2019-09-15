import logging


class Log:
    @staticmethod
    def create_logger(name: str = 'Logger', path: str = './logger.log',
                      level: str = 'INFO', mode: str = 'a', ):
        logger = logging.getLogger(name)
        if len(logger.handlers) == 0:
            logger.propagate = False
            formatter = logging.Formatter(fmt='[%(levelname)s]\t%(asctime)s %(message)s',
                                          datefmt='%Y-%m-%d %H:%M:%S')
            file_handler = logging.FileHandler(path, encoding='utf-8', mode=mode)
            file_handler.setFormatter(formatter)
            stream_handler = logging.StreamHandler()
            stream_handler.setFormatter(formatter)
            logger.addHandler(file_handler)
            logger.addHandler(stream_handler)
            logger.setLevel(level)
        return logger
