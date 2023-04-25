# cython: language_level=3
#!./env python
# -*- coding: utf-8 -*-
"""
# @Author           : Albert Wang
# @Time             : 2023-04-26 01:22:28
# @Description      :
# @Email            : shadowofgost@outlook.com
# @FilePath         : /Conclusion-Python/PublicFunctions.py
# @LastTime         : 2023-04-26 01:22:28
# @LastAuthor       : Albert Wang
# @Software         : Vscode
# @ Copyright Notice : Copyright (c) 2023 Albert Wang 王子睿, All Rights Reserved.
"""
from typing import Container, Optional, Type, NewType
from pydantic import BaseConfig, BaseModel, Field, create_model
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.inspection import inspect
from sqlalchemy.orm.properties import ColumnProperty
from .PublicSchema import format_current_time
Base = declarative_base()

exclude = [
    "back_up1",
    "back_up2",
    "back_up3",
    "back_up4",
    "back_up5",
    "back_up6",
    "IMark",
]
update_exclude = exclude[:]
update_exclude.extend(["TimeUpdate", "IdManager"])
insert_exclude = update_exclude[:]
insert_exclude.extend(["ID"])
select_out_exclude = exclude[:-1]
select_in_exclude = exclude[:]

nullable = ["Rem", "Introduction"]


class OrmConfig(BaseConfig):
    orm_mode = True


def sqlalchemy_to_pydantic(
    db_model: Type,
    exclude: Container[str] = [],
    nullable: Container[str] = ["*"],
    config: Type = OrmConfig,
    table_name: Optional[str] = None,
) -> Type[BaseModel]:
    """
    sqlalchemy_to_pydantic [summary]
    [extended_summary]
    Args:
        db_model (Type): [description]
        config (Type, optional): [description]. Defaults to OrmConfig.
        exclude (Container[str], optional): [description]. Defaults to [].
        nullable (Container[str], optional): [description]. Defaults to ["*"].
    Returns:
        Type[BaseModel]: [description]
    """

    mapper = inspect(db_model)
    fields = {}
    for attr in mapper.attrs:
        if isinstance(attr, ColumnProperty):
            if attr.columns:
                name = attr.key
                if name in exclude:
                    continue
                column = attr.columns[0]
                python_type = None
                if hasattr(column.type, "impl"):
                    if hasattr(column.type.impl, "python_type"):
                        python_type = column.type.impl.python_type
                elif hasattr(column.type, "python_type"):
                    python_type = column.type.python_type
                ##assert python_type, f"Could not infer python_type for {column}"
                default = None
                description = " "
                title = " "
                max_length = None
                if column.default is None and not column.nullable:
                    default = ...
                if column.doc is not None:
                    description = column.doc
                if column.comment is not None:
                    title = column.comment
                if hasattr(column.type, "length"):
                    max_length = column.type.length
                if nullable == ["*"]:
                    pass
                elif nullable == []:
                    python_type: Optional[type(python_type)]
                    default = None
                else:
                    if name in nullable:
                        pass
                    else:
                        python_type: type(python_type)
                        default = ...
                fields[name] = (
                    python_type,
                    Field(
                        description=description,
                        max_length=max_length,
                        title=title,
                        default=default,
                    ),
                )
    if table_name is None:
        table_name = db_model.__name__
    pydantic_model = create_model(
        table_name, __config__=config, **fields  # type: ignore
    )
    return pydantic_model
