

class S3Reader:
    def read(self, bytes_batch: dict) -> list:
        """
        Read the provided bytes and return a Pandas DataFrame.

        :param bytes_batch: bytes
        :return: pandas.DataFrame
        """
        raise NotImplementedError
