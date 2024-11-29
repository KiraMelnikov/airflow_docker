import pandas


class Writer:
    def write(self, data: [pandas.DataFrame | bytes]) -> None:
        """
        Write the provided data to the destination location.

        :return: None
        """
        raise NotImplementedError

    def verify_upload_end(self) -> None:
        """
        Verify the upload is finalized. E.g. close the multipart_upload session.

        :return: None
        """
        pass
