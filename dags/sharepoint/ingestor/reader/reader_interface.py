import pandas


class Reader:
    def read(self, bytes_batch: bytes) -> pandas.DataFrame:
        """
        Read the provided bytes and return a Pandas DataFrame.

        :param bytes_batch: bytes
        :return: pandas.DataFrame
        """
        raise NotImplementedError
