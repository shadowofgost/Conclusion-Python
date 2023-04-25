# cython: language_level=3
#!./env python
# -*- coding: utf-8 -*-
"""
# @Author           : Albert Wang
# @Time             : 2023-04-26 01:27:50
# @Description      :
# @Email            : shadowofgost@outlook.com
# @FilePath         : /Conclusion-Python/PublicService.py
# @LastTime         : 2023-04-26 01:27:51
# @LastAuthor       : Albert Wang
# @Software         : Vscode
# @ Copyright Notice : Copyright (c) 2023 Albert Wang 王子睿, All Rights Reserved.
"""
from sqlalchemy.orm import Session
from loguru import logger
from .ORM import ORM
from .PublicValue import model_dict
from .PublicSchema import (
    DeleteMultipleGetSchema,
    DeleteMultipleTableSchema,
    DeleteSingleTableSchema,
)
from Components import error_service_validation, error_service_null
from typing import List


def transform(id_manager: int, single_schema, multiple_schema, initial_schema):
    if initial_schema.n == 1:
        try:
            result_schema = single_schema(
                IdManager=id_manager, **initial_schema.data[0].dict()
            )
        except Exception:
            logger.error("service层表单转化验证失败")
            raise error_service_validation
    else:
        try:
            transform_list = [
                single_schema(IdManager=id_manager, **initial_schema.data[i].dict())
                for i in range(initial_schema.n)
            ]
        except Exception:
            logger.error("service层表单转化验证失败")
            raise error_service_validation
        try:
            result_schema = multiple_schema(data=transform_list, n=initial_schema.n)
        except Exception:
            logger.error("service层表单转化验证失败")
            raise error_service_validation
    return result_schema


def service_delete(
    session: Session, id_manager: int, model: str, schema: DeleteMultipleGetSchema
) -> dict[str, str]:
    model_instance = ORM(model, session)
    table_schema = transform(
        id_manager, DeleteSingleTableSchema, DeleteMultipleTableSchema, schema
    )
    if schema.n == 1:
        result = model_instance.delete(table_schema)
    else:
        result = model_instance.delete(table_schema, multiple=True)
    return result


def service_update(
    session: Session, id_manager: int, model: str, schema
) -> dict[str, str]:
    model_instance = ORM(model, session)
    single_schema = model_dict[model]["update_single_schema"]
    multiple_schema = model_dict[model]["update_multiple_schema"]
    table_schema = transform(id_manager, single_schema, multiple_schema, schema)
    if schema.n == 1:
        result = model_instance.update(table_schema)
    else:
        result = model_instance.update(table_schema, multiple=True)
    return result


def service_insert(
    session: Session, id_manager: int, model: str, schema
) -> dict[str, str]:
    model_instance = ORM(model, session)
    single_schema = model_dict[model]["insert_single_schema"]
    multiple_schema = model_dict[model]["insert_multiple_schema"]
    table_schema = transform(id_manager, single_schema, multiple_schema, schema)
    if schema.n == 1:
        result = model_instance.insert(table_schema)
    else:
        result = model_instance.insert(table_schema, multiple=True)
    return result


def service_select(
    session: Session, model: str, service_type: int, schema
) -> List[dict]:
    """
    select [筛选函数的服务提供]
    [查询的服务提供]
    Parameters
    ----------
    session : Session
        [description]
    id_manager : int
        [description]
    model : str
        [description]
    service_type : int
        [根据输入数据判断服务类型,0表示查询所有的数据，1表示查询的是通过id查询数据，2表示通过name查询数据，3表示通过schema查询特定值的数据，4表示通过学号/序列号查询账户]
    schema : [type], optional
        [description], by default None
    name : [type], optional
        [description], by default None
    Returns
    -------
    dict[str,str]
        [description]
    """
    model_instance = ORM(model, session)
    if service_type == 0:
        return model_instance.condition_select()
    elif service_type == 1:
        return model_instance.multiple_require_select(schema)
    elif service_type == 2:
        return model_instance.name_select(schema.Name)
    elif service_type == 3:
        return model_instance.multiple_require_select(schema)
    elif service_type == 4:
        return model_instance.multiple_require_select(schema)
    else:
        logger.error("请求的服务不存在")
        raise error_service_null
