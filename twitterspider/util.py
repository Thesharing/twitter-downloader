class TokenReader:

    @staticmethod
    def from_local_file(path, encoding='utf-8'):
        with open(path, 'r', encoding=encoding) as f:
            return f.readline().strip()
