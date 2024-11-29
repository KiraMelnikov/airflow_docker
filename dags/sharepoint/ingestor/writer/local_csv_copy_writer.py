from writer.writer_interface import Writer


class LocalCsvCopyWriter(Writer):
    def __init__(self, kwargs):
        self.copy_local_path = kwargs['copy_local_path']
        self.counter = 0

    def write(self, bytes_batch: bytes):
        with open(f'{self.copy_local_path}/csv-part-{self.counter}.csv', "wb") as file:
            file.write(bytes_batch)
            self.counter += 1
