from read_smx_sheet.app_Lib import functions as funcs
from read_smx_sheet.Logging_Decorator import Logging_decorator
from read_smx_sheet.parameters import parameters as pm
import datetime as dt


@Logging_decorator
def history_apply(cf, source_output_path, smx_table):
    print("hi")
    template_path = cf.templates_path + "/" + pm.default_bteq_oi_stg_template_file_name
    template_smx_path = cf.smx_path + "/" + "Templates" + "/" + pm.default_bteq_oi_stg_template_file_name
    loadDB_prefix = cf.ld_prefix
    modelDB_prefix = cf.modelDB_prefix
    modelDUP_prefix = cf.modelDup_prefix
    template_string = ""
    try:
        template_file = open(template_path, "r")
    except:
        template_file = open(template_smx_path, "r")

    for i in template_file.readlines():
        if i != "":
            template_string = template_string + i

    history_handeled = funcs.get_sama_stg_tables(smx_table, None)
    for stg_tables_df_index, stg_tables_df_row in stg_tables_df.iterrows():
        Table_name = stg_tables_df_row['Table_Name']
        schema_name = stg_tables_df_row['Schema_Name']
        f = funcs.WriteFile(source_output_path, Table_name, "bteq")
        filename = Table_name+'.bteq'
        stg_columns = funcs.get_sama_stg_table_columns_comma_separated(STG_tables, Table_name, 'STG')
        table_columns = funcs.get_sama_stg_table_columns_comma_separated(STG_tables, Table_name)
        stg_equal_datamart_pk = funcs.get_conditional_stamenet(STG_tables, Table_name, 'pk', '=', 'stg', 'dm')
        stg_equal_updt_cols = funcs.get_conditional_stamenet(STG_tables, Table_name, 'stg', '=', None, 'dm')

        if stg_equal_datamart_pk != '':
            stg_equal_datamart_pk = "ON" + stg_equal_datamart_pk


        bteq_script = template_string.format(currentdate=dt.datetime.now(),
                                             filename = filename,
                                             bteq_run_file=bteq_run_file, stg_prefix=stg_prefix,
                                             dm_prefix=data_mart_prefix,
                                             schema_name=schema_name,
                                             table_name=Table_name, stg_columns=stg_columns,
                                             stg_equal_datamart_pk=stg_equal_datamart_pk,
                                             stg_equal_updt_cols=stg_equal_updt_cols,
                                             table_columns=table_columns
                                             )

        bteq_script = bteq_script.upper()
        f.write(bteq_script)
        f.close()
