import pandas as pd
import pyarrow as pa
from deltalake import write_deltalake

from utils import get_kwarg_or_default
from writer.writer_interface import Writer

DELTA_WRITE_MODE_DEFAULT = 'append'


class LocalDeltaWriter(Writer):
    def __init__(self, kwargs):
        self.local_path = kwargs['local_path']
        self.delta_write_mode = get_kwarg_or_default('delta_write_mode', kwargs, DELTA_WRITE_MODE_DEFAULT)

    def write(self, df: pd.DataFrame) -> None:
        pa_table = pa.Table.from_pandas(df=df)
        write_deltalake(self.local_path, pa_table, mode=self.delta_write_mode)
