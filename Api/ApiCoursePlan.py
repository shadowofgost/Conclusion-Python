# cython: language_level=3
#!./env python
# -*- coding: utf-8 -*-
"""
# @Author           : Albert Wang
# @Time             : 2023-04-26 01:50:31
# @Description      :
# @Email            : shadowofgost@outlook.com
# @FilePath         : /Conclusion-Python/ApiCoursePlan.py
# @LastTime         : 2023-04-26 01:50:31
# @LastAuthor       : Albert Wang
# @Software         : Vscode
# @ Copyright Notice : Copyright (c) 2023 Albert Wang 王子睿, All Rights Reserved.
"""
from fastapi import APIRouter, Depends
from fastapi_pagination import Page, paginate, Params
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from Services import (
    get_course_plan,
    service_insert,
    service_update,
    service_delete,
    ModelCoursePlanSelectInSingleTableSchema,
    ModelCoursePlanSelectOutSingleTableSchema,
    ModelCoursePlanInsertMultipleGetSchema,
    ModelCoursePlanUpdateMultipleGetSchema,
    DeleteMultipleGetSchema,
    SchemaUserPydantic,
    Execution,
)
from .Depends import get_current_user, get_db

router = APIRouter(
    prefix="/model_courseplan",
    tags=["model_courseplan"],
)


class CurriculaGet(BaseModel):
    requires: ModelCoursePlanSelectInSingleTableSchema
    service_type: int = Field(
        title="选择查询的服务形式",
        description="根据输入数据判断服务类型,0表示查询所有的数据，1表示查询的是通过id查询数据，2表示通过name查询数据，3表示通过schema查询特定值的数据，4表示通过学号/序列号查询账户",
        default=0,
    )
    page: int = Field(default=1, title="页数", description="查询用的页码数")
    size: int = Field(default=5, title="记录的数量", description="一个页面查询记录的数量")


@router.post(
    "/search", response_model=Page[ModelCoursePlanSelectOutSingleTableSchema]
)
async def api_model_courseplan_get(
    schema: CurriculaGet,
    session: Session = Depends(get_db),
    user: SchemaUserPydantic = Depends(get_current_user),
):
    result_data = get_course_plan(
        session, user.Attr, schema.service_type, schema.requires
    )
    params = Params(page=schema.page, size=schema.size)
    return paginate(result_data, params)


@router.post("/", response_model=Execution)
async def api_model_courseplan_insert(
    schema: ModelCoursePlanInsertMultipleGetSchema,
    session: Session = Depends(get_db),
    user: SchemaUserPydantic = Depends(get_current_user),
):
    schema.n = len(schema.data)
    model = "ModelCoursePlan"
    return service_insert(session, user.ID, model, schema)


@router.put("/", response_model=Execution)
async def api_model_courseplan_update(
    schema: ModelCoursePlanUpdateMultipleGetSchema,
    session: Session = Depends(get_db),
    user: SchemaUserPydantic = Depends(get_current_user),
):
    schema.n = len(schema.data)
    model = "ModelCoursePlan"
    return service_update(session, user.ID, model, schema)


@router.delete("/", response_model=Execution)
async def api_model_courseplan_delete(
    schema: DeleteMultipleGetSchema,
    session: Session = Depends(get_db),
    user: SchemaUserPydantic = Depends(get_current_user),
):
    schema.n = len(schema.data)
    model = "ModelCoursePlan"
    return service_delete(session, user.ID, model, schema)
