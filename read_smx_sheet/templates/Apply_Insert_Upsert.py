from read_smx_sheet.app_Lib import functions as funcs
from read_smx_sheet.Logging_Decorator import Logging_decorator
from read_smx_sheet.parameters import parameters as pm


@Logging_decorator
def apply_insert_upsert(cf, source_output_path, SMX_SHEET, script_flag):
    print("here")
    if script_flag == 'Apply_Insert':
        file_name = 'Apply_Insert'
        f = funcs.WriteFile(source_output_path, file_name, "sql")
        template_path = cf.templates_path + "/" + pm.default_bteq_apply_insert_template_file_name
        template_smx_path = cf.smx_path + "/" + pm.default_bteq_apply_insert_template_file_name
    else:# script_flag == 'Apply_Upsert':
        file_name = 'Apply_Upsert'
        f = funcs.WriteFile(source_output_path, file_name, "sql")
        template_path = cf.templates_path + "/" + pm.default_bteq_apply_upsert_template_file_name
        template_smx_path = cf.smx_path + "/" + pm.default_bteq_apply_upsert_template_file_name



    # ld_prefix = cf.ld_prefix
    # fsdm_prefix = cf.fsdm_prefix
    # dup_prefix = cf.dup_prefix

    template_string = ""
    template_head = ""
    try:
        template_file = open(template_path, "r")
    except:
        template_file = open(template_smx_path, "r")
    template_start = 0
    template_head_line = 0

    for i in template_file.readlines():
        if i != "":
            if i[0] == '#' and template_head_line >= template_start:
                template_head = template_head+i
                template_head_line = template_head_line + 1
            else:
                template_string = template_string + i
                template_start = template_head_line+1


    f.write(template_head)
    record_ids_list = SMX_SHEET['Record_id'].unique()

    for record_id in record_ids_list:
        smx_record_id_df = funcs.get_sama_fsdm_record_id(SMX_SHEET, record_id)

        for model_tbls_df_index, model_tbls_df_row in smx_record_id_df.iterrows():
            Table_name = model_tbls_df_row['Entity'].str.strip()
            Record_id = model_tbls_df_row['Record_ID'].str.strip()
            ld_table_name = Table_name+"_"+Record_id



        #
        #     for model_tbl_columns_index, model_tbl__columns_row in model_table_columns.iterrows():
        #         Column_name = model_tbl__columns_row['Column']
        #         comma = '\t' + '  ,' if model_tbl_columns_index > 0 else ' '
        #         comma_Column_name = comma + Column_name
        #
        #         source_data_type = STG_table_columns_row['Data_Type']
        #         Data_type = source_data_type
        #         for data_type_index, data_type_row in Data_types.iterrows():
        #             if data_type_row['Source Data Type'].upper() == source_data_type.upper():
        #                 Data_type = str(data_type_row['Teradata Data Type'].upper())
        #         if str(STG_table_columns_row['Data_Length']) != '' and str(STG_table_columns_row['Data_Precision']) == '':
        #             Data_type = Data_type + "(" + str(STG_table_columns_row['Data_Length']) + ")"
        #
        #         elif str(STG_table_columns_row['Data_Length']) != '' and str(STG_table_columns_row['Data_Precision']) != '':
        #             source_data_type = source_data_type + "(" + str(STG_table_columns_row['Data_Length']) + ',' + str(
        #                 STG_table_columns_row['Data_Precision']) + ")"
        #             Data_type = source_data_type.replace("NUMBER", "DECIMAL")
        #
        #         if source_data_type == 'VARCHAR2':
        #             if STG_table_columns_row['Unicode_Flag'] == 'Y':
        #                 character_set = " CHARACTER SET UNICODE NOT CASESPECIFIC "
        #             else:
        #                 character_set = " CHARACTER SET LATIN NOT CASESPECIFIC "
        #         else:
        #             character_set = ""
        #
        #         if STG_table_columns_row['Primary_Key_Flag'].upper() == 'Y' or STG_table_columns_row[
        #             'Nullability_Flag'].upper() == 'N':
        #             not_null = " not null "
        #         else:
        #             not_null = ""
        #
        #         not_null = not_null if STG_table_columns_index == len(STG_table_columns) - 1 else not_null + '\n'
        #         columns = columns + comma_Column_name + " " + Data_type + character_set + not_null
        #
        #         if STG_table_columns_row['Primary_Key_Flag'].upper() == 'Y':
        #             pi_columns = pi_columns + ',' + Column_name if pi_columns != "" else Column_name
        #             if STG_table_columns_row['Teradata partition'].upper() == 'Y':
        #                 partition_columns = partition_columns + ',' + Column_name if partition_columns != "" \
        #                     else Column_name
        #
        #         if partition_columns != "":
        #             partition_by = "\nPartition by (" + partition_columns + ");" + "\n" + "\n"
        #         else:
        #             partition_by = ";" + "\n" + "\n"
        #
        #     if pi_columns != "":
        #         Table_name_pk = ' PI_'+Table_name
        #         pi_columns = "(" + pi_columns + ")"
        #         primary_index = "primary index "
        #         unique_primary_index = "unique primary index "
        #     else:
        #         Table_name_pk = ""
        #         pi_columns = ""
        #         primary_index = ""
        #         unique_primary_index = ""
        #
        #     if script_flag == 'Data_mart':
        #         create_stg_table = template_string.format(dm_prefix=data_mart_prefix, schema_name=schema_name,
        #                                                   table_name=Table_name, columns=columns,
        #                                                   pi_columns=pi_columns, partition_statement=partition_by,
        #                                                   primary_index=unique_primary_index,
        #                                                   Table_name_pk=Table_name_pk)
        #     elif script_flag == 'OI_staging':
        #         create_stg_table = template_string.format(oi_prefix=oi_prefix,  schema_name=schema_name,
        #                                                   table_name=Table_name, columns=columns,
        #                                                   pi_columns=pi_columns, primary_index=primary_index,
        #                                                   Table_name_pk=Table_name_pk)
        #     else:
        #         create_stg_table = template_string.format(stg_prefix=stg_prefix,
        #                                                   dup_suffix=dup_suffix, schema_name=schema_name,
        #                                                   table_name=Table_name, columns=columns,
        #                                                   pi_columns=pi_columns, primary_index=primary_index,
        #                                                   Table_name_pk=Table_name_pk)
        #     create_stg_table = create_stg_table.upper() + '\n' +'\n'
        #     f.write(create_stg_table)
        # f.close()