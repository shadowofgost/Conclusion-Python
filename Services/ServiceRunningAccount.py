# cython: language_level=3
#!./env python
# -*- coding: utf-8 -*-
"""
# @Author           : Albert Wang
# @Time             : 2023-04-26 01:43:03
# @Description      :
# @Email            : shadowofgost@outlook.com
# @FilePath         : /Conclusion-Python/ServiceRunningAccount.py
# @LastTime         : 2023-04-26 01:43:03
# @LastAuthor       : Albert Wang
# @Software         : Vscode
# @ Copyright Notice : Copyright (c) 2023 Albert Wang 王子睿, All Rights Reserved.
"""

    )
    result_courseplan = execute_database(sub_course_plan, session)
    id_curricula = result_courseplan[0]["ID_Curricula"]
    time_begin = result_courseplan[0]["TimeBegin"]
    time_end = result_courseplan[0]["TimeEnd"]
    ##上述操作是为了获取课程开始时间和结束时间
    sub_runningaccount = (
        select(ModelRunningAccount)
        .where(
            ModelRunningAccount.Type == 4097,
            ModelRunningAccount.IMark == 0,  # type: ignore
            ModelRunningAccount.Param2 == id_courseplan,
        )
        .subquery()
    )
    sub_user = select(ModelUser.ID, ModelUser.Name, ModelUser.NoUser).where(ModelUser.IMark == 0).where(ModelUser.ID == user.ID).subquery()  # type: ignore
    stmt = select(
        sub_runningaccount,
        sub_user.c.Name.label("ID_User_Name"),
        sub_user.c.NoUser.label("ID_User_NoUser"),
    ).outerjoin(sub_user, sub_runningaccount.c.ID_User == sub_user.c.ID)
    result = execute_database(stmt, session)
    if result == []:
        data = transform_into_schema(
            null_student,
            time_begin,
            time_end,
            id_courseplan,
        )
    else:
        data = transform_into_schema(result[0], time_begin, time_end, id_courseplan)
    data = update_status([data])
    return data


def get_for_teacher(session: Session, user: SchemaUserPydantic, id_courseplan: int):
    sub_course_plan = (
        select(
            ModelCoursePlan.ID_Curricula,  # type: ignore
            ModelCoursePlan.TimeBegin,
            ModelCoursePlan.TimeEnd,  # type: ignore
        )
        .where(ModelCoursePlan.ID == id_courseplan)
        .where(ModelCoursePlan.IMark == 0)
    )
    result_courseplan = execute_database(sub_course_plan, session)
    id_curricula = result_courseplan[0]["ID_Curricula"]
    time_begin = result_courseplan[0]["TimeBegin"]
    time_end = result_courseplan[0]["TimeEnd"]
    ##上述操作是为了获取课程开始时间和结束时间
    stmt = (
        select(ModelCurricula.RangeUsers)  # type: ignore
        .where(ModelCurricula.ID == id_curricula)
        .where(ModelCurricula.IMark == 0)
    )
    RangeUsers = execute_database(stmt, session)
    range_users_initial_data = RangeUsers[0]["RangeUsers"].strip().split(";")
    range_users = [int(i.strip()) for i in range_users_initial_data if i != ""]
    sub_runningaccount = (
        select(ModelRunningAccount)
        .where(ModelRunningAccount.Type == 4097)
        .where(ModelRunningAccount.IMark == 0)
        .where(ModelRunningAccount.Param2 == id_courseplan)
        .subquery()  # type: ignore
    )
    sub_user = (
        select(ModelUser)
        .where(ModelUser.IMark == 0)
        .where(ModelUser.NoUser.in_(range_users))
        .subquery()  # type: ignore
    )
    stmt = select(
        sub_user.c.ID.label("ID_User"),
        sub_user.c.Name.label("ID_User_Name"),
        sub_user.c.NoUser.label("ID_User_NoUser"),
        sub_runningaccount,
    ).outerjoin(sub_runningaccount, sub_runningaccount.c.ID_User == sub_user.c.ID)
    result_data = execute_database(stmt, session)
    data_initial_list = []
    for i in range(len(result_data)):
        data_initial_list.append(
            transform_into_schema(
                result_data[i],
                time_begin=time_begin,
                time_end=time_end,
                id_courseplan=id_courseplan,
            )
        )
    data = update_status(data_initial_list)
    return data


def get_for_admin(
    session: Session,
    user: SchemaUserPydantic,
    service_type: int,
    schema: ModelRunningAccountSelectInSingleTableSchema,
):
    pass


def get_running_account(
    session: Session,
    user: SchemaUserPydantic,
    schema: ModelRunningAccountSelectInSingleTableSchema,
):
    """
    获取用户的运行账户
    """
    if user.Attr == 1 or user.Attr == 2:
        return get_for_teacher(session, user, schema.Param2)
    elif user.Attr == 3:
        return get_for_student(session, user, schema.Param2)
    else:
        return get_for_student(session, user, schema.Param2)
