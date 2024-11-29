import io

import pandas as pd

from reader.reader_interface import Reader
from utils import get_kwarg_or_default

CSV_DELIMITER_DEFAULT = ','


class CsvReader(Reader):
    def __init__(self, kwargs: dict):
        self.delimiter = get_kwarg_or_default('csv_delimiter', kwargs, CSV_DELIMITER_DEFAULT)
        self.date_cols = get_kwarg_or_default('csv_date_cols', kwargs, None)
        self.col_names = get_kwarg_or_default('csv_col_names', kwargs, None)

    def read(self, bytes_batch: bytes) -> pd.DataFrame:
        df = pd.read_csv(io.BytesIO(bytes_batch),
                         delimiter=self.delimiter,
                         parse_dates=self.date_cols,
                         names=self.col_names)
        if self.col_names is None:
            self.col_names = df.columns
        return df
