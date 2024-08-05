import configparser
from pyspark import SparkConf
from typing import Dict


def get_config(env: str) -> Dict[str, str]:
    config = configparser.ConfigParser()
    config.read("conf/sbdl.conf")
    conf: Dict[str, str] = {}
    for (key, val) in config.items(env):
        conf[key] = val
    return conf


def get_spark_conf(env: str) -> SparkConf:
    spark_conf = SparkConf()
    config = configparser.ConfigParser()
    config.read("conf/spark.conf")

    for (key, val) in config.items(env):
        spark_conf.set(key, val)
    return spark_conf


def get_data_filter(env: str, data_filter: str) -> str:
    conf = get_config(env)
    return "true" if conf[data_filter] == "" else conf[data_filter]
