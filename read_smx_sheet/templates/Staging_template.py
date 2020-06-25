from read_smx_sheet.app_Lib import functions as funcs
from read_smx_sheet.Logging_Decorator import Logging_decorator
from read_smx_sheet.parameters import parameters as pm


@Logging_decorator
def stg_temp_DDL(cf, source_output_path, STG_tables, Data_types, script_flag):
    if script_flag == 'Data_mart':
        file_name = 'Data_mart_template'
        f = funcs.WriteFile(source_output_path, file_name, "sql")
        template_path = cf.templates_path + "/" + pm.default_data_mart_template_file_name
        template_smx_path = cf.smx_path + "/" + "Templates" + "/" + pm.default_data_mart_template_file_name
    else:
        file_name = funcs.get_file_name(__file__)
        f = funcs.WriteFile(source_output_path, file_name, "sql")
        template_path = cf.templates_path + "/" + pm.default_staging_template_file_name
        template_smx_path = cf.smx_path + "/" + "Templates" + "/" + pm.default_staging_template_file_name

    oi_prefix = cf.oi_prefix
    stg_prefix = cf.stg_prefix
    dup_suffix = cf.duplicate_table_suffix
    data_mart_prefix = cf.dm_prefix
    template_string = ""
    try:
        template_file = open(template_path, "r")
    except:
        template_file = open(template_smx_path, "r")

    for i in template_file.readlines():
        if i != "":
            if i[0] != '#':
                template_string = template_string + i

    stg_tables_df = funcs.get_sama_stg_tables(STG_tables, None)
    for stg_tables_df_index, stg_tables_df_row in stg_tables_df.iterrows():
        Table_name = stg_tables_df_row['Table_Name']
        schema_name = stg_tables_df_row['Schema_Name']
        STG_table_columns = funcs.get_sama_stg_table_columns(STG_tables, Table_name)
        pi_columns = ""
        partition_columns = ""
        columns = ""
        partition_by = ""

        for STG_table_columns_index, STG_table_columns_row in STG_table_columns.iterrows():
            Column_name = STG_table_columns_row['Column_Name']
            comma = '\t' + ',' if STG_table_columns_index > 0 else ' '
            comma_Column_name = comma + Column_name

            source_data_type = STG_table_columns_row['Data_Type']
            Data_type = source_data_type
            if str(STG_table_columns_row['Data_Length']) != '' and str(STG_table_columns_row['Data_Precision']) == '':
                source_data_type = source_data_type + "(" + str(STG_table_columns_row['Data_Length']) + ")"
                Data_type = source_data_type
            elif str(STG_table_columns_row['Data_Length']) != '' and str(STG_table_columns_row['Data_Precision']) != '':
                source_data_type = source_data_type + "(" + str(STG_table_columns_row['Data_Length']) + ',' + str(
                    STG_table_columns_row['Data_Precision']) + ")"
                Data_type = source_data_type.replace("NUMBER", "DECIMAL")

            for data_type_index, data_type_row in Data_types.iterrows():
                if data_type_row['Source Data Type'] == source_data_type:
                    Data_type = str(data_type_row['Teradata Data Type'])

            if source_data_type == 'VARCHAR2':
                if STG_table_columns_row['Data_Type'] == 'Y':
                    character_set = " CHARACTER SET UNICODE NOT CASESPECIFIC "
                else:
                    character_set = " CHARACTER SET LATIN NOT CASESPECIFIC "
            else:
                character_set = ""

            if STG_table_columns_row['Primary_Key_Flag'].upper() == 'Y' or STG_table_columns_row[
                'Nullability_Flag'].upper() == 'N':
                not_null = " not null "
            else:
                not_null = ""

            not_null = not_null if STG_table_columns_index == len(STG_table_columns) - 1 else not_null + '\n'
            columns = columns + comma_Column_name + " " + Data_type + character_set + not_null

            if STG_table_columns_row['Primary_Key_Flag'].upper() == 'Y':
                pi_columns = pi_columns + ',' + Column_name if pi_columns != "" else Column_name
                if STG_table_columns_row['Teradata partition'].upper() == 'Y':
                    partition_columns = partition_columns + ',' + Column_name if partition_columns != "" \
                        else Column_name

            if partition_columns != "":
                partition_by = "\nPartition by (" + partition_columns + ");" + "\n" + "\n"
            else:
                partition_by = ";" + "\n" + "\n"

        if pi_columns != "":
            Table_name_pk = Table_name
            pi_columns = "(" + pi_columns + ")"
            primary_index = "primary index "
        else:
            Table_name_pk = ""
            pi_columns = ""
            primary_index = ""

        if script_flag == 'Data_mart':
            create_stg_table = template_string.format(dm_prefix=data_mart_prefix, schema_name=schema_name,
                                                      table_name=Table_name, columns=columns,
                                                      pi_columns=pi_columns, partition_statement=partition_by,
                                                      primary_index=primary_index,
                                                      Table_name_pk=Table_name_pk)
        else:
            create_stg_table = template_string.format(oi_prefix=oi_prefix, stg_prefix=stg_prefix,
                                                      dup_suffix=dup_suffix, schema_name=schema_name,
                                                      table_name=Table_name, columns=columns,
                                                      pi_columns=pi_columns, partition_statement=partition_by,
                                                      primary_index=primary_index, Table_name_pk=Table_name_pk)
        f.write(create_stg_table)

    f.close()