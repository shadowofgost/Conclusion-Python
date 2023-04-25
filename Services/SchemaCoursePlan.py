# cython: language_level=3
#!./env python
# -*- coding: utf-8 -*-
"""
# @Author           : Albert Wang
# @Time             : 2023-04-26 01:38:47
# @Description      :
# @Email            : shadowofgost@outlook.com
# @FilePath         : /Conclusion-Python/SchemaCoursePlan.py
# @LastTime         : 2023-04-26 01:38:47
# @LastAuthor       : Albert Wang
# @Software         : Vscode
# @ Copyright Notice : Copyright (c) 2023 Albert Wang 王子睿, All Rights Reserved.
"""
from typing import List, Optional

from Models import (
    ModelCoursePlan,
    ModelCurricula,
    ModelLocation,
    ModelUser,
)
from pydantic import BaseModel, Field
from sqlalchemy import select
from sqlalchemy.orm import aliased

from .PublicFunctions import (
    format_current_time,
    insert_exclude,
    nullable,
    select_in_exclude,
    select_out_exclude,
    sqlalchemy_to_pydantic,
    update_exclude,
)

ModelCoursePlan_nullable_columns = [
    "RangeUsers",
    "ListDepts",
    "RangeEqus",
    "ListPlaces",
    "MapUser2Equ",
    "AboutSpeaker",
]
ModelCoursePlan_nullable_columns.extend(nullable)

ModelCoursePlanUpdateSingleGetSchemaBase = sqlalchemy_to_pydantic(
    ModelCoursePlan,
    update_exclude,
    table_name="ModelCoursePlanUpdateSingleGetSchemaBase",
)


class ModelCoursePlanUpdateSingleGetSchema(
    ModelCoursePlanUpdateSingleGetSchemaBase
):
    ID_Speaker_NoUser: str

    class Config:
        orm_mode = True


class ModelCoursePlanUpdateMultipleGetSchema(BaseModel):
    data: List[ModelCoursePlanUpdateSingleGetSchema]
    n: int


class ModelCoursePlanUpdateSingleTableSchema(
    ModelCoursePlanUpdateSingleGetSchemaBase
):
    TimeUpdate: int = format_current_time()
    IdManager: int
    IMark: int = 0


class ModelCoursePlanUpdateMultipleTableSchema(BaseModel):
    data: List[ModelCoursePlanUpdateSingleTableSchema]
    n: int


ModelCoursePlanInsertSingleGetSchemaBase = sqlalchemy_to_pydantic(
    ModelCoursePlan,
    insert_exclude,
    ModelCoursePlan_nullable_columns,
    table_name="ModelCoursePlanInsertSingleGetSchemaBase",
)


class ModelCoursePlanInsertSingleGetSchema(
    ModelCoursePlanInsertSingleGetSchemaBase
):
    ID_Speaker_NoUser: str

    class Config:
        orm_mode = True


class ModelCoursePlanInsertMultipleGetSchema(BaseModel):
    data: List[ModelCoursePlanInsertSingleGetSchema]
    n: int


class ModelCoursePlanInsertSingleTableSchema(
    ModelCoursePlanInsertSingleGetSchemaBase
):
    TimeUpdate: int = format_current_time()
    IdManager: int
    IMark: int = 0


class ModelCoursePlanInsertMultipleTableSchema(BaseModel):
    data: List[ModelCoursePlanInsertSingleTableSchema]
    n: int


ModelCoursePlanSelectOutSingleTableSchemaBase = sqlalchemy_to_pydantic(
    ModelCoursePlan,
    select_out_exclude,
    table_name="ModelCoursePlanSelectOutSingleTableSchemaBase",
)

ModelCoursePlanSelectInSingleTableSchema = sqlalchemy_to_pydantic(
    ModelCoursePlan,
    select_in_exclude,
    [],
    table_name="ModelCoursePlanSelectInSingleTableSchema",
)


class ModelCoursePlanSelectOutSingleTableSchema(
    ModelCoursePlanSelectOutSingleTableSchemaBase
):
    ID_Curricula_Name: Optional[str] = Field(
        default=None, title="课程名称", description="这是这节课程安排课对应的课程名称"
    )
    ID_Location_Name: Optional[str] = Field(
        default=None, title="地点名称", description="这是这节课程安排课对应的地点名称"
    )
    ID_Speaker_Name: Optional[str] = Field(
        default=None, title="老师名称", description="这是这节课程安排课对应的上课老师的姓名"
    )
    ID_Speaker_NoUser: Optional[str] = Field(
        default=None, title="老师/演讲者的教职工号", description="这是这节课程安排课对应的老师/演讲者的教职工号"
    )
    ID_Manager_Name: Optional[str] = Field(
        default=None, title="修改者的姓名", description="这次课程信息最新一次修改的修改者姓名"
    )

    class Config:
        orm_mode = True


##TODO:修改sql查询语句因为超过三个join使得mysql的性能会大幅下降。
sub_course_plan = select(ModelCoursePlan).where(ModelCoursePlan.IMark == 0).subquery()  # type: ignore
sub_curricula = select(ModelCurricula.ID, ModelCurricula.Name).where(ModelCurricula.IMark == 0).subquery()  # type: ignore
sub_location = select(ModelLocation.ID, ModelLocation.Name).where(ModelLocation.IMark == 0).subquery()  # type: ignore
sub_user = select(ModelUser.ID, ModelUser.Name, ModelUser.NoUser).where(ModelUser.IMark == 0).subquery()  # type: ignore
user1 = aliased(sub_user)
ModelCoursePlan_sub_stmt = (
    select(
        sub_course_plan,
        sub_curricula.c.Name.label("ID_Curricula_Name"),
        sub_location.c.Name.label("ID_Location_Name"),  # type: ignore
        sub_user.c.Name.label("ID_Speaker_Name"),
        sub_user.c.NoUser.label("ID_Speaker_NoUser"),
        user1.c.Name.label("ID_Manager_Name"),
    )
    .outerjoin(
        sub_curricula, sub_curricula.c.ID == sub_course_plan.c.ID_Curricula
    )
    .outerjoin(sub_user, sub_user.c.ID == sub_course_plan.c.ID_Speaker)
    .outerjoin(sub_location, sub_location.c.ID == sub_course_plan.c.ID_Location)
    .outerjoin(user1, user1.c.ID == sub_course_plan.c.IdManager)  # type: ignore
    .subquery()  # type: ignore
)
