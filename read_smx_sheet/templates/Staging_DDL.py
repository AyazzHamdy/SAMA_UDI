from read_smx_sheet.app_Lib import functions as funcs
from read_smx_sheet.Logging_Decorator import Logging_decorator
from read_smx_sheet.parameters import parameters as pm


@Logging_decorator
def stg_temp_DDL(cf, source_output_path, STG_tables, Data_types, script_flag):
    if script_flag == 'Data_mart':
        file_name = 'DATA_MART_DDL'
        f = funcs.WriteFile(source_output_path, file_name, "sql")
        template_path = cf.templates_path + "/" + pm.default_data_mart_template_file_name
        template_smx_path = cf.smx_path + "/" + pm.default_data_mart_template_file_name
    elif script_flag == 'OI_staging':
        file_name = 'OI_STAGING_DDL'
        f = funcs.WriteFile(source_output_path, file_name, "sql")
        template_path = cf.templates_path + "/" + pm.oi_staging_template_file_name
        template_smx_path = cf.smx_path + "/" + pm.oi_staging_template_file_name
    else:
        file_name = 'STAGING_DDL'
        f = funcs.WriteFile(source_output_path, file_name, "sql")
        template_path = cf.templates_path + "/" + pm.default_staging_template_file_name
        template_smx_path = cf.smx_path + "/" + pm.default_staging_template_file_name

    oi_prefix = cf.oi_prefix
    stg_prefix = cf.stg_prefix
    dup_suffix = cf.duplicate_table_suffix
    data_mart_prefix = cf.dm_prefix
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

    stg_tables_df = funcs.get_sama_stg_tables(STG_tables, None)
    f.write(template_head)
    for stg_tables_df_index, stg_tables_df_row in stg_tables_df.iterrows():
        Table_name = stg_tables_df_row['TABLE_NAME']
        schema_name = stg_tables_df_row['SCHEMA_NAME']
        STG_table_columns = funcs.get_sama_stg_table_columns(STG_tables, Table_name)
        pi_columns = ""
        partition_columns = ""
        columns = ""

        for STG_table_columns_index, STG_table_columns_row in STG_table_columns.iterrows():
            Column_name = STG_table_columns_row['COLUMN_NAME']
            comma = '\t' + '  ,' if STG_table_columns_index > 0 else ' '
            comma_Column_name = comma + Column_name

            source_data_type = STG_table_columns_row['DATA_TYPE']
            Data_type = source_data_type
            if str(STG_table_columns_row['DATA_LENGTH']) != '' and str(STG_table_columns_row['DATA_PRECISION']) == '':
                Data_type = Data_type + "(" + str(STG_table_columns_row['DATA_LENGTH']) + ")"
                for data_type_index, data_type_row in Data_types.iterrows():
                    if data_type_row['SOURCE DATA TYPE'].upper() == Data_type.upper():
                        Data_type = str(data_type_row['TERADATA DATA TYPE'].upper())
            elif str(STG_table_columns_row['DATA_LENGTH']) != '' and str(STG_table_columns_row['DATA_PRECISION']) != '':
                source_data_type = source_data_type + "(" + str(STG_table_columns_row['DATA_LENGTH']) + ',' + str(
                    STG_table_columns_row['DATA_PRECISION']) + ")"
                Data_type = source_data_type.replace("NUMBER", "DECIMAL")




            if source_data_type == 'VARCHAR2':
                if STG_table_columns_row['UNICODE_FLAG'] == 'Y':
                    character_set = " CHARACTER SET UNICODE NOT CASESPECIFIC "
                else:
                    character_set = " CHARACTER SET LATIN NOT CASESPECIFIC "
            else:
                character_set = ""

            if STG_table_columns_row['PRIMARY_KEY_FLAG'].upper() == 'Y' or STG_table_columns_row[
                'NULLABILITY_FLAG'].upper() == 'N':
                not_null = " not null "
            else:
                not_null = ""

            not_null = not_null if STG_table_columns_index == len(STG_table_columns) - 1 else not_null + '\n'
            columns = columns + comma_Column_name + " " + Data_type + character_set + not_null

            if STG_table_columns_row['PRIMARY_KEY_FLAG'].upper() == 'Y':
                pi_columns = pi_columns + ',' + Column_name if pi_columns != "" else Column_name
                if STG_table_columns_row['TERADATA_PARTITION'].upper() == 'Y':
                    partition_columns = partition_columns + ',' + Column_name if partition_columns != "" \
                        else Column_name

            if partition_columns != "":
                partition_by = "\nPartition by (" + partition_columns + ");" + "\n" + "\n"
            else:
                partition_by = ";" + "\n" + "\n"

        if pi_columns != "":
            Table_name_pk = ' PI_'+Table_name
            pi_columns = "(" + pi_columns + ")"
            primary_index = "primary index "
            unique_primary_index = "unique primary index "
        else:
            Table_name_pk = ""
            pi_columns = ""
            primary_index = ""
            unique_primary_index = ""

        if script_flag == 'Data_mart':
            create_stg_table = template_string.format(dm_prefix=data_mart_prefix, schema_name=schema_name,
                                                      table_name=Table_name, columns=columns,
                                                      pi_columns=pi_columns, partition_statement=partition_by,
                                                      primary_index=unique_primary_index,
                                                      Table_name_pk=Table_name_pk)
        elif script_flag == 'OI_staging':
            create_stg_table = template_string.format(oi_prefix=oi_prefix,  schema_name=schema_name,
                                                      table_name=Table_name, columns=columns,
                                                      pi_columns=pi_columns, primary_index=primary_index,
                                                      Table_name_pk=Table_name_pk)
        else:
            create_stg_table = template_string.format(stg_prefix=stg_prefix,
                                                      dup_suffix=dup_suffix, schema_name=schema_name,
                                                      table_name=Table_name, columns=columns,
                                                      pi_columns=pi_columns, primary_index=primary_index,
                                                      Table_name_pk=Table_name_pk)
        create_stg_table = create_stg_table.upper() + '\n' +'\n'
        f.write(create_stg_table)
    f.close()
