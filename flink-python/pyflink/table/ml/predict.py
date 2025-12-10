from typing import List

from pyflink.common.types import Row

from pyflink.table.udf import TableFunction, udtf


class PredictFunction(TableFunction):

    def eval(self, *args):
        input_row = Row(*args)
        results = self.predict(input_row)
        for result_row in results:
            yield result_row

    def predict(self, data: Row) -> List[Row]:
        """
        Performs prediction on the input data.

        :param data: The input data for prediction.
        :return: A list of rows containing the prediction results.
        """
        raise NotImplementedError


    @classmethod
    def create_udtf(cls):
        return udtf(cls())
