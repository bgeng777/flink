from typing import List
import pandas as pd
from pandas import Series

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


class BatchPredictFunction(TableFunction):

    def eval(self, *args):
        data = pd.DataFrame(
            {f"col{i}": col for i, col in enumerate(args)}
        )
        return self.predict(data)

    def predict(self, data: pd.DataFrame) -> pd.DataFrame:
        raise NotImplementedError


    @classmethod
    def create_udtf(cls):
        return udtf(cls())
