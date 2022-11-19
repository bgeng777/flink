from pyflink.common import typeinfo
from pyflink.datastream import DataStream, ProcessFunction
from pyflink.datastream.cep.condition import Condition
from pyflink.datastream.cep.pattern import Pattern


def pattern(ds: DataStream, pattern: Pattern):
    if not isinstance(pattern.condition, Condition) and not callable(pattern.condition):
        raise TypeError("The input must be a pattern.condition or a callable function")

    class FilterProcessFunctionAdapter(ProcessFunction):

        def __init__(self, filter_func: Condition):
            if isinstance(filter_func, Condition):
                self._open_func = None
                self._close_func = None
                self._filter_func = filter_func.filter
            else:
                self._open_func = None
                self._close_func = None
                self._filter_func = filter_func

        def filter(self, value):
            if self._filter_func.filter(value):
                yield value

    output_type = typeinfo._from_java_type(
        ds._j_data_stream.getTransformation().getOutputType())
    return ds.process(FilterProcessFunctionAdapter(pattern.condition), output_type=output_type) \
        .name("Cep")
