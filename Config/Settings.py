# cython: language_level=3
#!./env python
# -*- coding: utf-8 -*-
"""
# @Author           : Albert Wang
# @Time             : 2023-04-26 02:03:59
# @Description      :
# @Email            : shadowofgost@outlook.com
# @FilePath         : /Conclusion-Python/Config/Settings.py
# @LastTime         : 2023-04-26 02:04:00
# @LastAuthor       : Albert Wang
# @Software         : Vscode
# @ Copyright Notice : Copyright (c) 2023 Albert Wang 王子睿, All Rights Reserved.
"""
import os
from datetime import timedelta
from functools import lru_cache

from pydantic import BaseSettings

IsDev = True


class DefaultSettings(BaseSettings):
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    SECRET_KEY = (
        "09d25e094faa6ca2556c818166b7a9563b93f7099f6f0f4caa6cf63b88e8d3e7"
    )
    ALGORITHM = "HS256"
    TOKEN_EXPIRE_MINUTES = timedelta(minutes=120)
    TOKEN_LIFETIME = timedelta(hours=24)
    DB_ABS_PATH = os.path.join(BASE_DIR, "database/sqlite.db")
    DATABASE_URL = f"sqlite+aiosqlite:///{DB_ABS_PATH}"
    # Set True if database is SQLite otherwise False
    RENDER_AS_BATCH = True
    LOGIN_URL = "/auth/login"
    REDIS_HOST = "127.0.0.1"
    REDIS_PORT = 6379


class Dev(BaseSettings):
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    SECRET_KEY = (
        "09d25e094faa6ca2556c818166b7a9563b93f7099f6f0f4caa6cf63b88e8d3e7"
    )
    ALGORITHM = "HS256"
    TOKEN_EXPIRE_MINUTES = timedelta(minutes=120)
    TOKEN_LIFETIME = timedelta(hours=24)
    DATABASE_TYPE = "mysql"
    DATABASE_CONNECT = "pymysql"
    DATABASE_NAME = "WebBackend"
    DATABASE_USER_NAME = "web"
    DATABASE_USER_PASSWORD = "123456"
    DATABASE_HOST = "101.132.135.180"
    DATABASE_PORT = "3306"
    ENCODE_TYPE = "utf8"
    DATABASE_URL = "{}+{}://{}:{}@{}:{}/{}?charset={}".format(
        DATABASE_TYPE,
        DATABASE_CONNECT,
        DATABASE_USER_NAME,
        DATABASE_USER_PASSWORD,
        DATABASE_HOST,
        DATABASE_PORT,
        DATABASE_NAME,
        ENCODE_TYPE,
    )
    LOGIN_URL = "/auth/login"
    REDIS_HOST = "127.0.0.1"
    REDIS_PORT = 6379


class Production(BaseSettings):
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    SECRET_KEY = (
        "09d25e094faa6ca2556c818166b7a9563b93f7099f6f0f4caa6cf63b88e8d3e7"
    )
    ALGORITHM = "HS256"
    TOKEN_EXPIRE_MINUTES = timedelta(minutes=120)
    TOKEN_LIFETIME = timedelta(hours=24)
    DATABASE_TYPE = "mysql"
    DATABASE_CONNECT = "pymysql"
    DATABASE_NAME = "WebBackend"
    DATABASE_USER_NAME = "web"
    DATABASE_USER_PASSWORD = "123456"
    DATABASE_HOST = "101.132.135.180"
    DATABASE_PORT = "3306"
    ENCODE_TYPE = "utf8"
    DATABASE_URL = "{}+{}://{}:{}@{}:{}/{}?charset={}".format(
        DATABASE_TYPE,
        DATABASE_CONNECT,
        DATABASE_USER_NAME,
        DATABASE_USER_PASSWORD,
        DATABASE_HOST,
        DATABASE_PORT,
        DATABASE_NAME,
        ENCODE_TYPE,
    )
    LOGIN_URL = "/auth/login"
    REDIS_HOST = "127.0.0.1"
    REDIS_PORT = 6379


@lru_cache()
def get_settings():
    if IsDev == True:
        result_return = Dev()
        return result_return
    else:
        result_return = Production()
        return result_return
