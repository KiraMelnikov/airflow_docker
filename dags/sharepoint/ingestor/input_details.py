BATCH_SIZE_DEFAULT = 1024 ** 2 * 128  # 128 MiB


class InputDetails:
    def __init__(self, data_format: str = None,
                 **kwargs):
        self.data_format = data_format
        self.kwargs = kwargs

class InputDetailsLoader(InputDetails):
    def __init__(self, data_source:str = None,
                        on_aws:bool = None,
                        to_sign:bool = None,
                        **kwargs) -> None:
        self.data_source = data_source
        self.on_aws = on_aws
        self.to_sign = to_sign
        self.kwargs = kwargs