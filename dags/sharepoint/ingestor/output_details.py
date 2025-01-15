class OutputDetails:
    def __init__(self,
                 destination,
                 data_format,
                 make_source_copy=False,
                 **kwargs):
        self.make_source_copy = make_source_copy
        self.data_format = data_format
        self.destination = destination
        self.kwargs = kwargs
