from pyflink.common import typeinfo, Types
from pyflink.datastream import DataStream, ProcessFunction, KeyedProcessFunction, RuntimeContext
from pyflink.datastream.cep.condition import Condition
from pyflink.datastream.cep.pattern import Pattern


def pattern(ds: DataStream, pattern: Pattern):
    if not isinstance(pattern.condition, Condition) and not callable(pattern.condition):
        raise TypeError("The input must be a pattern.condition or a callable function")

    class MyFilterProcessFunctionAdapter(KeyedProcessFunction):
        def open(self, runtime_context: RuntimeContext):
            self.my_process_element = self._my_process_element

        def process_element(self, value, ctx: 'KeyedProcessFunction.Context'):
            return self._process_element(value, ctx)

        def my_process_element(self, value, ctx: 'KeyedProcessFunction.Context'):
            return self._my_process_element(value, ctx)

        def _process_element(self, value, ctx: 'KeyedProcessFunction.Context'):
            # if self._filter_func.filter(value):
            yield self._filter_func2.filter(value)

        def _my_process_element(self, value, ctx: 'KeyedProcessFunction.Context'):
            # if self._filter_func.filter(value):
            yield self._filter_func1.filter(value)

        def __init__(self, input_pattern: Pattern):
            if isinstance(input_pattern, Pattern):
                self._open_func = None
                self._close_func = None
                self._filter_func1 = input_pattern.condition
                if input_pattern.prev is not None:
                    self._filter_func2 = input_pattern.prev.condition
                else:
                    self._filter_func2 = None
            else:
                raise TypeError("Must be Pattern type.")

    output_type = typeinfo._from_java_type(
        ds._j_data_stream.getTransformation().getOutputType())
    return ds.key_by(lambda x: x[0], key_type=Types.STRING()).cep_process(MyFilterProcessFunctionAdapter(pattern), j_pattern=pattern.j_pattern, output_type=output_type) \
        .name("Cep")
