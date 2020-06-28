from read_smx_sheet.app_Lib import functions as funcs
from read_smx_sheet.Logging_Decorator import Logging_decorator
from read_smx_sheet.parameters import parameters as pm


@Logging_decorator
def bteq_temp_script(cf, source_output_path, STG_tables):
    template_path = cf.templates_path + "/" + pm.default_bteq_template_file_name
    template_smx_path = cf.smx_path + "/" + "Templates" + "/" + pm.default_bteq_template_file_name
    stg_prefix = cf.stg_prefix
    dup_suffix = cf.duplicate_table_suffix
    data_mart_prefix = cf.dm_prefix
    bteq_run_file = cf.bteq_run_file
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
    for stg_tables_df_index, stg_tables_df_row in stg_tables_df.iterrows():
        Table_name = stg_tables_df_row['Table_Name']
        schema_name = stg_tables_df_row['Schema_Name']
        f = funcs.WriteFile(source_output_path, Table_name, "bteq")
        stg_columns = funcs.get_sama_stg_table_columns_comma_separated(STG_tables, Table_name, 'STG')
        table_columns = funcs.get_sama_stg_table_columns_comma_separated(STG_tables, Table_name)
        stg_equal_datamart_pk = funcs.get_conditional_stamenet(STG_tables, Table_name, 'pk', '=', 'stg', 'datamart')
        alias = stg_prefix + '.' + Table_name
        table_equal_updt_pk = funcs.get_conditional_stamenet(STG_tables, Table_name, 'pk', '=',
                                                             alias, 'updt')
        stg_not_equal_datamart_cols = funcs.get_conditional_stamenet(STG_tables, Table_name, 'stg', '<>', 'stg',
                                                                     'datamart')
        stg_equal_updt_cols = funcs.get_conditional_stamenet(STG_tables, Table_name, 'stg', '=', None, 'updt')
        data_mart_pks_null = funcs.get_conditional_stamenet(STG_tables, Table_name, 'pk', 'NULL', 'datamart', None)

        if stg_equal_datamart_pk != '':
            stg_equal_datamart_pk = "ON " + stg_equal_datamart_pk
            data_mart_pks_null = "WHERE " + data_mart_pks_null
            table_equal_updt_pk = "WHERE " + table_equal_updt_pk

        bteq_script = template_string.format(bteq_run_file=bteq_run_file, stg_prefix=stg_prefix,
                                             dm_prefix=data_mart_prefix,
                                             dup_suffix=dup_suffix, schema_name=schema_name,
                                             table_name=Table_name, stg_columns=stg_columns,
                                             stg_equal_datamart_pk=stg_equal_datamart_pk,
                                             stg_not_equal_datamart_cols=stg_not_equal_datamart_cols,
                                             stg_equal_updt_cols=stg_equal_updt_cols,
                                             table_equal_updt_pk=table_equal_updt_pk,
                                             table_columns=table_columns,
                                             data_mart_pks_null=data_mart_pks_null
                                             )
        f.write(template_head)
        f.write(bteq_script)
        f.close()
