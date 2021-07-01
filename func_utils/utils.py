from pyspark.sql import SparkSession as Session
from pyspark import SparkConf as Conf
from pyspark.sql import functions as F, Window as W
from pyspark.sql import DataFrame, Window
from pyspark.sql.types import IntegerType

C = F.col


def format_columns_name(sdf: DataFrame) -> DataFrame:
    format_columns_name_sdf = (
        sdf.withColumnRenamed("時間", "Date")
        .withColumnRenamed("開盤價", "Open")
        .withColumnRenamed("最高價", "High")
        .withColumnRenamed("最低價", "Low")
        .withColumnRenamed("收盤價", "Close")
    )
    return format_columns_name_sdf


def format_value(sdf: DataFrame, replace_char="口") -> DataFrame:
    """將字元拿掉，並轉為IntegerType"""
    for column in sdf.columns[1:]:
        sdf = sdf.withColumn(
            column, F.regexp_replace(C(column), replace_char, "").cast(IntegerType())
        )
    return sdf


def extend_feature(sdf: DataFrame, columns=None, lag1=True) -> DataFrame:
    """輸入想要擴展特徵的column與擴展的特徵，可能未來可加上sma值等"""
    if lag1:
        columns = columns or sdf.columns[5:]
        if type(columns) != list:
            raise TypeError("columns must be list type")
        for column in columns:
            window = Window.partitionBy().orderBy("Date")
            sdf = (
                sdf.withColumn("prev_value", F.lag(column).over(window))
                .withColumn(
                    f"∆{column}",
                    F.when(F.isnull(C(column) - C("prev_value")), 0).otherwise(
                        C(column) - C("prev_value")
                    ),
                )
                .drop("prev_value")
            )
        sdf = (
            sdf.withColumn("index", F.row_number().over(window))
            .where(C("index") != 1)
            .drop("index")
        )

    return sdf


def train_val_split(df: DataFrame, ratio: float = 0.7) -> DataFrame:
    """切分樣本內與樣本外"""
    separation_point = round(len(df) * ratio)
    train_df = df.iloc[:separation_point]
    val_df = df.iloc[separation_point + 1 :]
    return train_df, val_df
