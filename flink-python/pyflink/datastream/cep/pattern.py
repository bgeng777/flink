from pyflink.datastream.cep.condition import Condition
from pyflink.java_gateway import get_gateway


class Pattern(object):

    def __init__(self, name: str):
        self.name = name
        self.gateway = get_gateway()
        JPattern = self.gateway.jvm.org.apache.flink.cep.pattern.Pattern
        self.j_pattern = JPattern(name)
        self.prev = None

    def times(self, times: int):
        self.j_pattern.times(times)
        return self

    def where(self, condition: Condition):
        self.condition = condition
        return self
        # conditionJSimplePythonCondition = self.gateway.jvm.org.apache.flink.streaming.cep\
        #     .SimplePythonCondition

    def followedBy(self, pattern):
        tmp_j_pattern = self.j_pattern.followedBy(pattern.name)
        # tmp_j_pattern = self.j_pattern.followedBy(pattern.j_pattern)
        pattern.prev = self
        # self = pattern
        pattern.j_pattern = tmp_j_pattern
        return pattern

