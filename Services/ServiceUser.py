# cython: language_level=3
#!./env python
# -*- coding: utf-8 -*-
"""
# @Author           : Albert Wang
# @Time             : 2023-04-26 01:40:12
# @Description      :
# @Email            : shadowofgost@outlook.com
# @FilePath         : /Conclusion-Python/ServiceUser.py
# @LastTime         : 2023-04-26 01:40:13
# @LastAuthor       : Albert Wang
# @Software         : Vscode
# @ Copyright Notice : Copyright (c) 2023 Albert Wang 王子睿, All Rights Reserved.
"""
from sqlalchemy.orm import Session
from .PublicService import service_select
from .SchemaUser import (
    ModelUserSelectInSingleTableSchema,
    ModelUserSelectOutSingleTableSchema,
)
from Components import UserNotFound
from typing import List, Optional
from loguru import logger


##TODO:WARNING:后端文件的进行的权限控制系统
class SchemaUserPydantic(ModelUserSelectOutSingleTableSchema):
    ID: int
    Attr: int  ##这个attr是对数据库attr的重新设定，1是管理员，2是教师，3是学生,4 是其他人
    NoUser: str
    Name: str
    Psw: str

    class Config:
        orm_mode = True


def modify_user_attr(user_list: List[dict]) -> SchemaUserPydantic:
    try:
        user = SchemaUserPydantic(**user_list[0])
    except Exception:
        logger.error("用户不存在")
        raise UserNotFound
    if user.NoUser.isdigit() == True:
        user.Attr = 3
    else:
        if user.Attr == 1 or user.Attr == 2:
            user.Attr = 1
        else:
            user.Attr = 2
    return user


def get_user_id(session: Session, id_manager: int) -> SchemaUserPydantic:
    schema = ModelUserSelectInSingleTableSchema(ID=id_manager)
    model = "ModelUser"
    user_list = service_select(session, model, 1, schema)
    return modify_user_attr(user_list)


def get_user_nouser(session: Session, nouser: str) -> SchemaUserPydantic:
    schema = ModelUserSelectInSingleTableSchema(NoUser=nouser)
    model = "ModelUser"
    user_list = service_select(session, model, 4, schema)
    return modify_user_attr(user_list)
