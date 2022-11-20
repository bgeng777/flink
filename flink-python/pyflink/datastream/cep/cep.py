from pyflink.common import typeinfo, Types
from pyflink.datastream import DataStream, ProcessFunction, KeyedProcessFunction
from pyflink.datastream.cep.condition import Condition
from pyflink.datastream.cep.pattern import Pattern


def pattern(ds: DataStream, pattern: Pattern):
    if not isinstance(pattern.condition, Condition) and not callable(pattern.condition):
        raise TypeError("The input must be a pattern.condition or a callable function")

    class MyFilterProcessFunctionAdapter(KeyedProcessFunction):

        def process_element(self, value, ctx: 'KeyedProcessFunction.Context'):
            # if self._filter_func.filter(value):
                yield self._filter_func.filter(value)

        def __init__(self, filter_func: Condition):
            if isinstance(filter_func, Condition):
                self._open_func = None
                self._close_func = None
                self._filter_func = filter_func
            else:
                self._open_func = None
                self._close_func = None
                self._filter_func = filter_func

    output_type = typeinfo._from_java_type(
        ds._j_data_stream.getTransformation().getOutputType())
    return ds.key_by(lambda x: x[0], key_type=Types.STRING()).cep_process(MyFilterProcessFunctionAdapter(pattern.condition), j_pattern=pattern.j_pattern, output_type=output_type) \
        .name("Cep")
