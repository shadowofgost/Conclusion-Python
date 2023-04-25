# cython: language_level=3
#!./env python
# -*- coding: utf-8 -*-
"""
# @Author           : Albert Wang
# @Time             : 2023-04-26 02:06:20
# @Description      :
# @Email            : shadowofgost@outlook.com
# @FilePath         : /Conclusion-Python/Config/logger_config.py
# @LastTime         : 2023-04-26 02:06:21
# @LastAuthor       : Albert Wang
# @Software         : Vscode
# @ Copyright Notice : Copyright (c) 2023 Albert Wang 王子睿, All Rights Reserved.
"""
import logging
from os.path import join, dirname
from sys import stdout
from time import strftime
from pprint import pformat
from types import FrameType
from typing import cast

from loguru import logger
from loguru._defaults import LOGURU_FORMAT

# -----------------------系统调试------------------------------------
DEBUG = True
# -----------------------日志-----------------------------------------


class InterceptHandler(logging.Handler):
    """
    Default handler from examples in loguru documentaion.
    See https://loguru.readthedocs.io/en/stable/overview.html#entirely-compatible-with-standard-logging
    """

    def emit(self, record: logging.LogRecord):
        # Get corresponding Loguru level if it exists
        try:
            level = logger.level(record.levelname).name
        except ValueError:
            level = record.levelno

        # Find caller from where originated the logged message
        frame, depth = logging.currentframe(), 2
        while frame.f_code.co_filename == logging.__file__:
            frame = cast(FrameType, frame.f_back)
            depth += 1

        logger.opt(depth=depth, exception=record.exc_info).log(
            level, record.getMessage()
        )


def format_record(record: dict) -> str:
    """
    这里的代码是copy的，记录日志格式的
    Custom format for loguru loggers.
    Uses pformat for log any data like request/response body during debug.
    Works with logging if loguru handler it.
    Example:
    # >>> payload = [{"users":[{"name": "Nick", "age": 87, "is_active": True}, {"name": "Alex", "age": 27, "is_active": True}], "count": 2}]
    # >>> logger.bind(payload=).debug("users payload")
    # >>> [   {   'count': 2,
    # >>>         'users': [   {'age': 87, 'is_active': True, 'name': 'Nick'},
    # >>>                      {'age': 27, 'is_active': True, 'name': 'Alex'}]}]
    """

    format_string = LOGURU_FORMAT
    if record["extra"].get("payload") is not None:
        record["extra"]["payload"] = pformat(
            record["extra"]["payload"], indent=4, compact=True, width=88
        )
        format_string += "\n<level>{extra[payload]}</level>"  # type: ignore
    format_string += "{exception}\n"  # type: ignore
    return format_string


def make_filter(name: str):
    # 过滤操作，当日志要选择对应的日志文件的时候，通过filter进行筛选
    def filter_(record):
        return record["extra"].get("name") == name

    return filter_


def init_logging():
    """
    loggers = (
        logging.getLogger(name)
        for name in logging.root.manager.loggerDict
        if name.startswith("uvicorn.")
    )
    for uvicorn_logger in loggers:
        uvicorn_logger.handlers = [InterceptHandler()]
    """
    # 这里的操作是为了改变uvicorn默认的logger，使之采用loguru的logger
    # change handler for default uvicorn logger
    logging.getLogger().handlers = [InterceptHandler()]
    # set logs output, level and format
    # logger.add(sys.stdout, level=logging.DEBUG, format=format_record, filter=make_filter('stdout'))
    # 为app添加一个info log文件，主要记录debug和info级别的日志
    dirpath = dirname(dirname(__file__)) + "/Logs"
    app_info = join(dirpath, f'{strftime("%Y_%m_%d")+"_info"}.log')
    # 为app添加一个error log文件，主要记录warning和error级别的日志
    app_warning = join(dirpath, f'{strftime("%Y_%m_%d")+"_warning_error"}.log')
    logger.add(
        app_info,
        enqueue=True,
        rotation="12:00",
        level=logging.INFO,
        retention="120 days",
    )
    logger.add(
        app_warning,
        enqueue=True,
        rotation="12:00",
        level=logging.WARNING,
        retention="120 days",
    )
    # 配置loguru的日志句柄，sink代表输出的目标
    logger.configure(
        handlers=[
            {"sink": stdout, "level": logging.DEBUG, "format": format_record},
            {
                "sink": app_info,
                "level": logging.INFO,
                "format": format_record,
            },
            {
                "sink": app_warning,
                "level": logging.WARNING,
                "format": format_record,
            },
        ]
    )
    return logger
