from pyflink.datastream.cep.condition import Condition
from pyflink.java_gateway import get_gateway


class Pattern(object):

    def __init__(self, name: str):
        self.name = name
        self.gateway = get_gateway()
        JPattern = self.gateway.jvm.org.apache.flink.cep.pattern.Pattern.Pattern
        self.j_pattern = JPattern(name)

    def where(self, condition: Condition):
        self.condition = condition
        # conditionJSimplePythonCondition = self.gateway.jvm.org.apache.flink.streaming.cep\
        #     .SimplePythonCondition

