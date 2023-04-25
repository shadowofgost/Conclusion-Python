# cython: language_level=3
#!./env python
# -*- coding: utf-8 -*-
"""
# @Author           : Albert Wang
# @Time             : 2023-04-26 01:21:04
# @Description      :
# @Email            : shadowofgost@outlook.com
# @FilePath         : /Conclusion-Python/ORM.py
# @LastTime         : 2023-04-26 01:57:09
# @LastAuthor       : Albert Wang
# @Software         : Vscode
# @ Copyright Notice : Copyright (c) 2023 Albert Wang 王子睿, All Rights Reserved.
"""
from typing import List
from sqlalchemy import create_engine, delete, insert, select, update
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import Session, sessionmaker
from loguru import logger
from .PublicValue import model_dict, name_column_model_dict
from Components import (
    error_database_execution,
    error_schema_validation,
    success_execution,
)

Base = declarative_base()
session = 1
stmt_format = type(select())


## 所有查询的默认前提条件是接收到的数据不包含IMark字段的数据
def execution(stmt, session: Session) -> dict[str, str]:
    """
    execution  [执行的查询语言]
    [根据传入的参数进行查询]
    Args:
        stmt ([type]): [需要执行的stmt语句]
        session (Session): [数据库执行所需要的链接]
    Returns:
        dict: [返回执行结果成功或者失败的信息]
    """
    try:
        session.execute(stmt)
        session.commit()
        session.flush()
        return success_execution
    except Exception:
        session.rollback()
        logger.error("数据库执行出错，查看数据库数据情况")
        raise error_database_execution


def single_table_multiple_require_select(
    sub_select,
    schema,
    session: Session,
    physical: bool = False,
    offset_data: int = -1,
    limit_data: int = -1,
) -> List[dict]:
    """
    single_table_multiple_require_select [特殊选择的表]
    [当传入的参数中只有id的时候，说明查询id情况下的取值]
    Args:
        sub_select ([type]): [业务查询或者model，表示直接查类的查询]
        schema ([type]): [接受的表，比如user的schema，id=1，name=。。。。]
        session (Session): [description]
        physical (bool, optional): [显示的是逻辑表还是物理表，False表示逻辑表，True表示物理表]. Defaults to False.
        offset_data (int, optional): [sql中的偏移量，从偏移量+1的位置开始选择数据，实现分页功能]. Defaults to -1.
        limit_data (int, optional): [sql中选择的数据总数，表示每一页数据的总的个数]. Defaults to -1.
    Returns:
        [type]: [description]
    """
    schema_origin = schema.dict()
    schema_dict = {
        key: value
        for key, value in schema_origin.items()
        if value != None and value != "" and value != "string"
    }
    if physical == False:
        schema_dict["IMark"] = 0
    if limit_data == -1 or offset_data == -1:
        stmt = select(sub_select).filter_by(**schema_dict)  # type: ignore
    else:
        stmt = (
            select(sub_select)
            .filter_by(**schema_dict)  # type: ignore
            .offset(offset_data)
            .limit(limit_data)
        )
    try:
        return session.execute(stmt).mappings().all()
    except Exception:
        logger.error("数据库执行出错，查看数据库数据情况")
        raise error_database_execution


def single_table_condition_select(
    sub_select,
    session: Session,
    condition: str = "",
    physical: bool = False,
    offset_data: int = -1,
    limit_data: int = -1,
) -> List[dict]:
    """
    single_table_condition_select [根据传入的条件进行查询]
    [根据传入的参数condition进行查询，condition的格式为：str，举例为：“ModelUser.id>=1,ModelUser.sex==2”这种,如果没有传入参数condition或者传入的是“”，那么查询所有值]
    Args:
        sub_select ([type]): [业务查询或者model，表示直接查类的查询]
        session (Session): [description]
        condition (str, optional): [传入的参数]. Defaults to "".
        physical (bool, optional): [显示的是逻辑表还是物理表，False表示逻辑表，True表示物理表]. Defaults to False.
        offset_data (int, optional): [sql中的偏移量，从偏移量+1的位置开始选择数据，实现分页功能]. Defaults to -1.
        limit_data (int, optional): [sql中选择的数据总数，表示每一页数据的总的个数]. Defaults to -1.
    Returns:
        [type]: [description]
    """
    ##TODO:WARNING:Salchemy的迁移，subquery,也就是子查询会更改接口形式，原来的sub.c.colum改为sub.column形式，注意修改
    if hasattr(sub_select, "c"):
        imark_condition = sub_select.c.IMark == 0
    else:
        imark_condition = sub_select.IMark == 0
    if limit_data == -1 or offset_data == -1:
        if physical == False:
            if condition == "" or condition == None:
                ##TODO:WARNING:这里的查询是逻辑表的查询，需要改进
                stmt = select(sub_select).where(imark_condition)
            else:
                stmt = (
                    select(sub_select)
                    .where(eval(condition))
                    .where(imark_condition)
                )
        else:
            if condition == "" or condition == None:
                stmt = select(sub_select)
            else:
                stmt = select(sub_select).where(eval(condition))
    else:
        if physical == False:
            if condition == "" or condition == None:
                stmt = (
                    select(sub_select)
                    .where(imark_condition)
                    .offset(offset_data)
                    .limit(limit_data)
                )
            else:
                stmt = (
                    select(sub_select)
                    .where(eval(condition))
                    .where(imark_condition)
                    .offset(offset_data)
                    .limit(limit_data)
                )
        else:
            if condition == "" or condition == None:
                stmt = select(sub_select).offset(offset_data).limit(limit_data)
            else:
                stmt = (
                    select(sub_select)
                    .where(eval(condition))
                    .offset(offset_data)
                    .limit(limit_data)
                )
    try:
        return session.execute(stmt).mappings().all()
    except Exception:
        logger.error("数据库执行出错，查看数据库数据情况")
        raise error_database_execution


def single_table_name_select(
    sub_select,
    session: Session,
    name: str,
    physical: bool = False,
    offset_data: int = -1,
    limit_data: int = -1,
) -> List[dict]:
    ##TODO:WARNING:Salchemy的迁移，subquery,也就是子查询会更改接口形式，原来的sub.c.colum改为sub.column形式，注意修改
    """
    single_table_condition_select [根据传入的条件进行查询]
    [根据传入的参数condition进行查询，condition的格式为：str，举例为：“ModelUser.id>=1,ModelUser.sex==2”这种,如果没有传入参数condition或者传入的是“”，那么查询所有值]
    Args:
        sub_select ([type]): [业务查询或者model，表示直接查类的查询]
        session (Session): [description]
        name (str, optional): [需要查询的名字,近似查询].
        physical (bool, optional): [显示的是逻辑表还是物理表，False表示逻辑表，True表示物理表]. Defaults to False.
        offset_data (int, optional): [sql中的偏移量，从偏移量+1的位置开始选择数据，实现分页功能]. Defaults to -1.
        limit_data (int, optional): [sql中选择的数据总数，表示每一页数据的总的个数]. Defaults to -1.
    Returns:
        [type]: [description]
    """
    name = "%" + name + "%"
    if hasattr(sub_select, "c"):
        name_condition = sub_select.c.Name.like(name)
    else:
        name_condition = sub_select.Name.like(name)
    if limit_data == -1 or offset_data == -1:
        if physical == False:
            stmt = (
                select(sub_select)
                .where(name_condition)
                .where(sub_select.c.IMark == 0)
            )
        else:
            stmt = select(sub_select).where(name_condition)
    else:
        if physical == False:
            stmt = (
                select(sub_select)
                .where(name_condition)
                .where(sub_select.c.IMark == 0)
                .offset(offset_data)
                .limit(limit_data)
            )
        else:
            stmt = (
                select(sub_select)
                .where(name_condition)
                .offset(offset_data)
                .limit(limit_data)
            )
    try:
        return session.execute(stmt).mappings().all()
    except Exception:
        logger.error("数据库执行出错，查看数据库数据情况")
        raise error_database_execution


def single_insert(model, schema, session: Session):
    stmt = insert(model).values(**schema.dict())
    return execution(stmt, session)


def multiple_insert(model, multiple_schema, session: Session):
    mappings = [
        multiple_schema.data[i].dict() for i in range(multiple_schema.n)
    ]
    """
    另一种写法，users是表名，data是数据
    data = [
    {"id": 1, "name": "John", "age": 30},
    {"id": 2, "name": "Michael", "age": 40},
    {"id": 3, "name": "Sarah", "age": 25},
]
    stmt = users.insert().values(data)
    session.execute(stmt)
    session.commit()
    """
    try:
        session.bulk_insert_mappings(model, mappings)
        session.commit()
        session.flush()
        return success_execution
    except Exception:
        session.rollback()
        logger.error("数据库执行出错，查看数据库数据情况")
        raise error_database_execution


def single_update(model, schema, session: Session):
    stmt = update(model).where(model.ID == schema.ID).values(**schema.dict())
    return execution(stmt, session)


def multiple_update(model, multiple_schema, session: Session):
    mappings = [
        multiple_schema.data[i].dict() for i in range(multiple_schema.n)
    ]
    """
    另一种写法，users是表名，data是数据
    data = {
        1: {"name": "John", "age": 30},
        3: {"name": "Michael", "age": 40},
        5: {"name": "Sarah", "age": 25},
    }
    stmt = (
        update(users)
        .where(users.c.id.in_(data.keys()))
        .values(
            name=case(
                value=users.c.id,
                whens={
                    id: data[id]["name"]
                    for id in data.keys() if "name" in data[id]
                },
                else_=users.c.name,
            ),
            age=case(
                value=users.c.id,
                whens={
                    id: data[id]["age"]
                    for id in data.keys() if "age" in data[id]
                },
                else_=users.c.age,
            ),
        )
    )
    session.execute(stmt)
    session.commit()
    """

    try:
        session.bulk_update_mappings(model, mappings)
        session.commit()
        session.flush()
        return success_execution
    except Exception:
        session.rollback()
        logger.error("数据库执行出错，查看数据库数据情况")
        raise error_database_execution


def single_delete(model, schema, session: Session):
    stmt = (
        update(model)
        .where(model.ID == schema.ID)
        .values(
            IMark=1, TimeUpdate=schema.TimeUpdate, IdManager=schema.IdManager
        )
    )
    return execution(stmt, session)


def single_delete_physical(model, schema, session: Session):
    stmt = delete(model).where(model.ID == schema.ID)
    return execution(stmt, session)


def multiple_delete(model, multiple_schema, session: Session):
    mappings = [
        {
            "ID": multiple_schema.data[i].ID,
            "TimeUpdate": multiple_schema.data[i].TimeUpdate,
            "IdManager": multiple_schema.data[i].IdManager,
            "IMark": 1,
        }
        for i in range(multiple_schema.n)
    ]
    try:
        session.bulk_update_mappings(model, mappings)
        session.commit()
        session.flush()
        return success_execution
    except Exception:
        session.rollback()
        logger.error("数据库执行出错，查看数据库数据情况")
        raise error_database_execution


def multiple_delete_physical(model, multiple_schema, session: Session):
    id_list=[multiple_schema.data[i].ID for i in range(multiple_schema.n)]
    try:
        stmt = delete(model).where(model.c.id.in_(id_list))
        session.execute(stmt)
        session.commit()
        session.flush()
        return success_execution
    except Exception:
        session.rollback()
        logger.error("数据库执行出错，查看数据库数据情况")
        raise error_database_execution


def get_session():
    SQLALCHEMY_DATABASE_URL = (
        "mysql+pymysql://web:123456@101.132.135.180:3306/LiveStream?"
    )
    # SQLALCHEMY_DATABASE_URL = "postgresql://user:password@postgresserver/db"
    engine = create_engine(SQLALCHEMY_DATABASE_URL)
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    return SessionLocal()


class ORM:
    def __init__(
        self, model: str, session: Session, abstract_model: bool = False
    ):
        """
        __init__ORM初始化,用于初始化具体的表的增删改查操作
        [extended_summary]
        Args:
            model (str): [表名，或者说model名，必须存在在model_dict中]
            session (Session, optional): [启动用于提交的数据库session]. Defaults to None.
            abstract_model (bool, optional): [是否是抽象的表，比如水平分表的总和抽象表，比如User表]. Defaults to False.
        """
        if session == None:
            self.session = get_session()
        else:
            self.session = session
        if abstract_model == True:
            self.model = model_dict[model]["model"](model, session=self.session)
            self.abstract_model = abstract_model
        else:
            if model in name_column_model_dict.keys():
                self.has_name = True
            else:
                self.has_name = False
            self.abstract_model = abstract_model
            self.model = model_dict[model]["model"]
            self.delete_single_schema = model_dict[model][
                "delete_single_schema"
            ]
            self.insert_single_schema = model_dict[model][
                "insert_single_schema"
            ]
            self.update_single_schema = model_dict[model][
                "update_single_schema"
            ]
            self.select_single_schema = model_dict[model][
                "select_single_schema"
            ]
            self.delete_multiple_schema = model_dict[model][
                "delete_multiple_schema"
            ]
            self.insert_multiple_schema = model_dict[model][
                "insert_multiple_schema"
            ]
            self.update_multiple_schema = model_dict[model][
                "update_multiple_schema"
            ]
            self.select_multiple_schema = model_dict[model][
                "select_multiple_schema"
            ]
            self.service_sub_stmt = model_dict[model]["service_sub_stmt"]

    def multiple_require_select(
        self,
        schema,
        service: bool = True,
        physical: bool = False,
        offset_data: int = -1,
        limit_data: int = -1,
    ) -> List[dict]:
        """
        multiple_require_select [特殊选择查询]
        [针对一系列要求的值对表进行的查询]
        Args:
            schema ([type]): [接受的表，比如user的schema，id=1，name=。。。。]
            service (bool, optional): [表示是否是业务查询，默认是True表示默认是提供业务形式的查询结果，False表示直接提供表结构的查询结果]. Defaults to True.
            physical (bool, optional): [显示的是逻辑表还是物理表，False表示逻辑表，True表示物理表]. Defaults to False.
            offset_data (int, optional): [sql中的偏移量，从偏移量+1的位置开始选择数据，实现分页功能]. Defaults to -1.
            limit_data (int, optional): [sql中选择的数据总数，表示每一页数据的总的个数]. Defaults to -1.
        Returns:
            [dict]: [返回的是列表形式的值，报错或者查询结果参数]
        """
        if self.abstract_model == True:
            return self.model.multiple_require_select(
                schema, physical, offset_data, limit_data
            )
        else:
            try:
                select_schema = self.select_single_schema(**schema.dict())
            except Exception:
                logger.error("数据库用于执行的表单验证失败")
                raise error_schema_validation
            if service == True:
                return single_table_multiple_require_select(
                    self.service_sub_stmt,
                    select_schema,
                    self.session,
                    physical,
                    offset_data,
                    limit_data,
                )
            else:
                return single_table_multiple_require_select(
                    self.model,
                    select_schema,
                    self.session,
                    physical,
                    offset_data,
                    limit_data,
                )

    def condition_select(
        self,
        condition: str = "",
        service: bool = True,
        physical: bool = False,
        offset_data: int = -1,
        limit_data: int = -1,
    ) -> List[dict]:
        """
        condition_select [根据传入的条件进行查询]
        [根据传入的参数condition进行查询，condition的格式为：str，举例为：“ModelUser.id>=1,ModelUser.sex==2”这种,如果没有传入参数condition或者传入的是“”，那么查询所有值]
        Args:
            condition (str, optional): [传入的参数]. Defaults to "".
            service (bool, optional): [表示是否是业务查询，默认是True表示默认是提供业务形式的查询结果，False表示直接提供表结构的查询结果]. Defaults to True.
            physical (bool, optional): [显示的是逻辑表还是物理表，False表示逻辑表，True表示物理表]. Defaults to False.
            offset_data (int, optional): [sql中的偏移量，从偏移量+1的位置开始选择数据，实现分页功能]. Defaults to -1.
            limit_data (int, optional): [sql中选择的数据总数，表示每一页数据的总的个数]. Defaults to -1.
        Returns:
            [type]: [description]
        """
        if self.abstract_model == True:
            return self.model.condition_select(
                condition, service, physical, offset_data, limit_data
            )
        else:
            if service == True:
                return single_table_condition_select(
                    self.service_sub_stmt,
                    self.session,
                    condition,
                    physical,
                    offset_data,
                    limit_data,
                )
            else:
                return single_table_condition_select(
                    self.model,
                    self.session,
                    condition,
                    physical,
                    offset_data,
                    limit_data,
                )

    def name_select(
        self,
        name: str,
        service: bool = True,
        physical: bool = False,
        offset_data: int = -1,
        limit_data: int = -1,
    ) -> List[dict]:
        """
        condition_select [根据传入的条件进行查询]
        [根据传入的参数condition进行查询，condition的格式为：str，举例为：“ModelUser.id>=1,ModelUser.sex==2”这种,如果没有传入参数condition或者传入的是“”，那么查询所有值]
        Args:
            name(str, optional): [传入的name字符串,进行匹配查询name].
            service (bool, optional): [表示是否是业务查询，默认是True表示默认是提供业务形式的查询结果，False表示直接提供表结构的查询结果]. Defaults to True.
            physical (bool, optional): [显示的是逻辑表还是物理表，False表示逻辑表，True表示物理表]. Defaults to False.
            offset_data (int, optional): [sql中的偏移量，从偏移量+1的位置开始选择数据，实现分页功能]. Defaults to -1.
            limit_data (int, optional): [sql中选择的数据总数，表示每一页数据的总的个数]. Defaults to -1.
        Returns:
            [type]: [description]
        """
        if self.abstract_model == True:
            return self.model.name_select(
                name, service, physical, offset_data, limit_data
            )
        else:
            if self.has_name == False:
                logger.error("数据库用于执行的表单验证失败")
                raise error_schema_validation
            else:
                if service == True:
                    return single_table_condition_select(
                        self.service_sub_stmt,
                        self.session,
                        name,
                        physical,
                        offset_data,
                        limit_data,
                    )
                else:
                    return single_table_condition_select(
                        self.model,
                        self.session,
                        name,
                        physical,
                        offset_data,
                        limit_data,
                    )

    def insert(
        self,
        schema,
        multiple: bool = False,
    ):
        """
        insert [插入数据]
        [插入增加新的数据]
        Args:
            schema ([type]): [表示插入提交的表单数据]. Defaults to None.
            multiple (bool, optional): [判断是否是批量插入，如果是False表示不是批量插入，只是增加一个数据，如果是True表示是批量插入]. Defaults to False.
        Returns:
            [type]: [description]
        """

        if self.abstract_model == True:
            return self.model.insert(schema, multiple)
        else:
            if multiple == True:
                try:
                    insert_schema = self.insert_multiple_schema(**schema.dict())
                except Exception:
                    logger.error("数据库用于执行的表单验证失败")
                    raise error_schema_validation
                return multiple_insert(self.model, insert_schema, self.session)
            else:
                try:
                    insert_schema = self.insert_single_schema(**schema.dict())
                except Exception:
                    logger.error("数据库用于执行的表单验证失败")
                    raise error_schema_validation
                return single_insert(self.model, insert_schema, self.session)

    def update(
        self,
        schema,
        multiple: bool = False,
    ):
        """
        update [修改数据]
        [修改现有的数据]
        Args:
            schema ([type]): [表示插入提交的表单数据]. Defaults to None.
            multiple (bool, optional): [判断是否是批量插入，如果是False表示不是批量插入，只是增加一个数据，如果是True表示是批量插入]. Defaults to False.
        Returns:
            [type]: [description]
        """
        if self.abstract_model == True:
            return self.model.update(schema, multiple)
        else:
            if multiple == True:
                try:
                    update_schema = self.update_multiple_schema(**schema.dict())
                except Exception:
                    logger.error("数据库用于执行的表单验证失败")
                    raise error_schema_validation
                return multiple_update(self.model, update_schema, self.session)
            else:
                try:
                    update_schema = self.update_single_schema(**schema.dict())
                except Exception:
                    logger.error("数据库用于执行的表单验证失败")
                    raise error_schema_validation
                return single_update(self.model, update_schema, self.session)

    def delete(
        self,
        schema,
        multiple: bool = False,
        physical: bool = False,
    ):
        """
        delete [删除数据]
        [extended_summary]
        Args:
            schema ([type]): [表示插入提交的表单数据]. Defaults to None.
            multiple (bool, optional): [判断是否是批量插入，如果是False表示不是批量插入，只是增加一个数据，如果是True表示是批量插入]. Defaults to False.
            physical (bool, optional): [表示是否是物理删除数据，False表示非物理删除是逻辑删除，True表示是物理删除]. Defaults to False.
        Returns:
            [type]: [description]
        """
        if self.abstract_model == True:
            return self.model.delete(schema, multiple, physical)
        else:
            if multiple == True:
                try:
                    delete_schema = self.delete_multiple_schema(**schema.dict())
                except Exception:
                    logger.error("数据库用于执行的表单验证失败")
                    raise error_schema_validation
                if physical == True:
                    return multiple_delete_physical(
                        self.model, delete_schema, self.session
                    )
                else:
                    return multiple_delete(
                        self.model, delete_schema, self.session
                    )
            else:
                try:
                    delete_schema = self.delete_single_schema(**schema.dict())
                except Exception:
                    logger.error("数据库用于执行的表单验证失败")
                    raise error_schema_validation
                if physical == True:
                    return single_delete_physical(
                        self.model, delete_schema, self.session
                    )
                else:
                    return single_delete(
                        self.model, delete_schema, self.session
                    )
