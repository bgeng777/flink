# 文件名: split_udtf.py
# from pyflink.table import TableFunction, DataTypes
from pyflink.table.udf import TableFunction, ScalarFunction
from pyflink.table.udf import udtf,udf
from pyflink.table import DataTypes
from pyflink.table.udf import udtf


# @udtf(result_types=[DataTypes.STRING()])
# def SplitUDTF(s: str):
#     splits = s.split("|")
#     yield s.upper()
# @udtf(result_types=[DataTypes.STRING()])

# class SplitUDTF(TableFunction):
#
#     def eval(self, content: str):
#         """
#         核心逻辑：输入一个字符串，使用 yield 返回多行结果。
#         """
#         # if content:
#             # 假设以逗号分隔
#             # str.upper(content)
#             # for s in content.split(","):
#                 # yield 直接返回数据，Flink 会自动将其封装成 Row
#         return [content.upper()]
#
# # 显式导出，方便 Flink 识别（也可以直接在 SQL 中引用类名）
# split_func = udtf(SplitUDTF(), result_types=[DataTypes.STRING()])


class SplitUDTF(TableFunction):

    def eval(self, content: str):
        """
        核心逻辑：输入一个字符串，使用 yield 返回多行结果。
        """
        # if content:
            # 假设以逗号分隔
            # str.upper(content)
            # for s in content.split(","):
                # yield 直接返回数据，Flink 会自动将其封装成 Row
        return [content.upper()]

# 显式导出，方便 Flink 识别（也可以直接在 SQL 中引用类名）
split_func = udtf(SplitUDTF(), result_types=DataTypes.STRING())



@udtf(result_types=[DataTypes.STRING()])
def mysplit(s: str):
    splits = s.split("|")
    yield s+"?"