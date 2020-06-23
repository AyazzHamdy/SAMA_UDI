from read_smx_sheet.app_Lib import functions as funcs
from read_smx_sheet.Logging_Decorator import Logging_decorator
from read_smx_sheet.parameters import parameters as pm


@Logging_decorator
def data_mart_temp_DDL(cf,source_output_path,STG_tables, Data_types):
    file_name = funcs.get_file_name(__file__)
    f = funcs.WriteFile(source_output_path, file_name, "sql")
    stg_tables_df = funcs.get_sama_stg_tables(STG_tables, None)
    table_technical_columns = ',b_id INTEGER TITLE ' + "'B_Id'" + "\n" + ',file_name VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC' + "\n" + ',insrt_dttm TIMESTAMP(6) TITLE ' + "'Insert_Dttm'"  + "\n" + ',updt_dttm TIMESTAMP(6) TITLE ' + "'Update_Dttm'" + "\n"
    template_path = cf.smx_path + "/" + "Templates" + "/" + pm.default_data_mart_template_file_name
    separator = "$$$"
    template_path.replace(separator,"\n")
    template_string = ""
    try:
        template_file = open(template_path, "r")
    except:
        template_file = None

    for i in template_file.readlines():
        line = i.strip()
        if line != "":
            if line[0] != '#':
                template_string = template_string + line + "\n"

    print(template_string)

    for stg_tables_df_index, stg_tables_df_row in stg_tables_df.iterrows():
        Table_name = stg_tables_df_row['Table_Name']
        data_mart_prefix = cf.dm_prefix
        schema_Name = stg_tables_df_row['Schema_Name']
        STG_table_columns = funcs.get_sama_stg_table_columns(STG_tables, Table_name)
        pi_columns = ""
        columns = ""
        partition_columns = ""

        for STG_table_columns_index, STG_table_columns_row in STG_table_columns.iterrows():
            Column_name = STG_table_columns_row['Column_Name']
            comma = ',' if STG_table_columns_index > 0 else ' '
            comma_Column_name = comma + Column_name

            source_data_type = STG_table_columns_row['Data_Type']
            Data_type = source_data_type
            if STG_table_columns_row['Data_Length'] != '' and str(STG_table_columns_row['Data_Precision']) == '':
                source_data_type = source_data_type+"("+str(STG_table_columns_row['Data_Length'])+")"
            elif STG_table_columns_row['Data_Length'] != '' and STG_table_columns_row['Data_Precision'] != '':
                source_data_type = source_data_type + "(" + str(STG_table_columns_row['Data_Length']) + ',' + str(STG_table_columns_row['Data_Precision']) + ")"
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

            if STG_table_columns_row['Primary_Key_Flag'].upper() == 'Y' or STG_table_columns_row['Nullability_Flag'].upper() == 'N':
                not_null = " not null "
            else:
                not_null = ""

            columns = comma_Column_name + " " + Data_type + character_set + not_null + "\n"

            if STG_table_columns_row['Primary_Key_Flag'].upper() == 'Y':
                pi_columns = pi_columns + ',' + Column_name if pi_columns != "" else Column_name
            if STG_table_columns_row['Teradata partition'].upper() == 'Y':
                partition_columns = partition_columns + ',' + Column_name if partition_columns != "" else Column_name

        if pi_columns != "" and partition_columns == "":
            Primary_Index = ") UNIQUE PRIMARY INDEX PI_" + Table_name + "(" + pi_columns + ")\n"
        elif pi_columns != "" and partition_columns != "":
            Primary_Index = ") PRIMARY INDEX PI_" + Table_name + "(" + pi_columns + ")\n"
        else:
            Primary_Index = ")"

        if partition_columns != "":
            partition_by = "Partition by (" + partition_columns + ")\n"
        else:
            partition_by = " "

        #create_stg_table = create_stg_table + table_technical_columns + Primary_Index + partition_by
        #create_stg_table = create_stg_table + ";\n\n"
        create_stg_table = template_string.format(dm_prefix=data_mart_prefix,schema_name=schema_Name,table_name=Table_name,columns=columns,
                                                  technical_columns=table_technical_columns,pi_columns=pi_columns,partition_statement=partition_by) + "\n" + "\n" + "\n"
        f.write(create_stg_table)

    f.close()
