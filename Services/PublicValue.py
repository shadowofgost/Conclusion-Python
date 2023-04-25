# cython: language_level=3
#!./env python
# -*- coding: utf-8 -*-
"""
# @Author           : Albert Wang
# @Time             : 2023-04-26 01:32:07
# @Description      :
# @Email            : shadowofgost@outlook.com
# @FilePath         : /Conclusion-Python/PublicValue.py
# @LastTime         : 2023-04-26 01:32:08
# @LastAuthor       : Albert Wang
# @Software         : Vscode
# @ Copyright Notice : Copyright (c) 2023 Albert Wang 王子睿, All Rights Reserved.
"""
from os import listdir
from os.path import dirname
from re import match
from .PublicSchema import (
    DeleteSingleTableSchema,
    DeleteMultipleTableSchema,
    DeleteSingleGetSchema,
    DeleteMultipleGetSchema,
)

path = dirname(__file__)
model_filename_list = [
    filename_str.split(".")[0].replace("Schema", "Model")
    for filename_str in listdir(path)
    if filename_str.endswith(".py")
    and match(r"Schema", filename_str) is not None
]


def import_models_schemas():
    ##把Model下的所有符合要求的实体model类和schema导入到当前的模块中
    for model_filename in model_filename_list:
        exec(
            f"from .{model_filename.replace('Model','Schema')}  import ({model_filename} ,{model_filename}UpdateSingleGetSchema,{model_filename}UpdateMultipleGetSchema,{model_filename}UpdateSingleTableSchema,{model_filename}UpdateMultipleTableSchema,{model_filename}InsertSingleGetSchema,{model_filename}InsertMultipleGetSchema,{model_filename}InsertSingleTableSchema,{model_filename}InsertMultipleTableSchema,{model_filename}SelectOutSingleTableSchema,{model_filename}SelectInSingleTableSchema,{model_filename}_sub_stmt,)"  # noqa: E501
        )
        ##导入对应的模块
    return locals()


locals_data = import_models_schemas()
globals().update(locals_data)
name_column_model_list = [
    "ModelUser",
    "ModelTypera",
    "ModelTableInformation",
    "ModelLocation",
    "ModelEquipment",
    "ModelDepartment",
    "ModelCurricula",
]
model_dict = {}
name_column_model_dict = {}


def format_variables():
    for model_filename in model_filename_list:
        tmp_dict = {}
        tmp_dict["delete_single_schema"] = DeleteSingleTableSchema
        tmp_dict["delete_multiple_schema"] = DeleteMultipleTableSchema
        tmp_dict["delete_single_get_schema"] = DeleteSingleGetSchema
        tmp_dict["delete_multiple_get_schema"] = DeleteMultipleGetSchema
        tmp_dict["update_single_schema"] = eval(
            model_filename + "UpdateSingleTableSchema"
        )
        tmp_dict["update_multiple_schema"] = eval(
            model_filename + "UpdateMultipleTableSchema"
        )
        tmp_dict["update_single_get_schema"] = eval(
            model_filename + "UpdateSingleGetSchema"
        )
        tmp_dict["update_multiple_get_schema"] = eval(
            model_filename + "UpdateMultipleGetSchema"
        )
        tmp_dict["insert_single_schema"] = eval(
            model_filename + "InsertSingleTableSchema"
        )
        tmp_dict["insert_multiple_schema"] = eval(
            model_filename + "InsertMultipleTableSchema"
        )
        tmp_dict["insert_single_get_schema"] = eval(
            model_filename + "InsertSingleGetSchema"
        )
        tmp_dict["insert_multiple_get_schema"] = eval(
            model_filename + "InsertMultipleGetSchema"
        )
        tmp_dict["select_single_schema"] = eval(
            model_filename + "SelectInSingleTableSchema"
        )
        tmp_dict["select_multiple_schema"] = eval(
            model_filename + "SelectOutSingleTableSchema"
        )
        tmp_dict["service_sub_stmt"] = eval(model_filename + "_sub_stmt")
        tmp_dict["model"] = eval(model_filename + "")
        ##exec(f"from {model_filename} import {model_filename}")
        if model_filename in name_column_model_list:
            name_column_model_dict[model_filename] = eval(model_filename + "")
        model_dict[model_filename] = tmp_dict


format_variables()
