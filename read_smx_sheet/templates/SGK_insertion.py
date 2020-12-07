from read_smx_sheet.app_Lib import functions as funcs
from read_smx_sheet.Logging_Decorator import Logging_decorator
from read_smx_sheet.parameters import parameters as pm


@Logging_decorator
def sgk_insertion(cf, source_output_path, smx_table):
    template_path = cf.templates_path + "/" + pm.default_sgk_template_file_name
    template_smx_path = cf.smx_path + "/" + "Templates" + "/" + pm.default_sgk_template_file_name
    MODELDB = cf.modelDB_prefix
    STGDB = cf.stg_prefix
    bteq_run_file = cf.bteq_run_file
    SOURCENAME = cf.sgk_source
    current_date = funcs.get_current_date()
    template_string = ""
    SGK_tables = smx_table[smx_table['Entity'].str.endswith(str('_SGK'))]
    try:
        template_file = open(template_path, "r")
    except:
        template_file = open(template_smx_path, "r")

    for i in template_file.readlines():
        if i != "":
            template_string = template_string + i

    for SGK_tables_index, SGK_tables_row in SGK_tables.iterrows():
        RECORDID = SGK_tables_row['Record_ID']
        TABLENAME = SGK_tables_row['Entity'].upper()
        SOURCEDATABASE = SGK_tables_row['Stg_Schema'].upper()
        filename = SOURCENAME+'_'+TABLENAME+'_R'+str(RECORDID)
        f = funcs.WriteFile(source_output_path, filename, "bteq")
        filename = filename + '.bteq'
        TABLECOLUMNS = funcs.get_sama_table_columns_comma_separated(SGK_tables,TABLENAME,'sgk',RECORDID)
        SGKID = funcs.get_sgk_record(SGK_tables,TABLENAME,RECORDID,'sgk_id')
        NULLCOLUMNS = funcs.get_sgk_record(SGK_tables,TABLENAME,RECORDID,'null_cols')
        SOURCECOLUMN = funcs.get_sgk_record(SGK_tables,TABLENAME,RECORDID,'src_col')
        SOURCETABLE = funcs.get_sgk_record(SGK_tables,TABLENAME,RECORDID,'src_tbl')
        JOINRULE = funcs.get_sgk_record(SGK_tables,TABLENAME,RECORDID,'join_rule')
        FILTERRULE = funcs.get_sgk_record(SGK_tables,TABLENAME,RECORDID,'filter_rule')
        SGKKEY = funcs.get_sgk_record(SGK_tables,TABLENAME,RECORDID,'sgk_key')
        SOURCEKEY = funcs.get_sgk_record(SGK_tables,TABLENAME,RECORDID,'src_key')
        DATATYPE = funcs.get_sgk_record(SGK_tables,TABLENAME,RECORDID,'data_type')

        bteq_script = template_string.format(versionnumber=pm.ver_no,
                                             currentdate=current_date,
                                             filename=filename,
                                             bteq_run_file=bteq_run_file, MODELDB=MODELDB,
                                             TABLENAME=TABLENAME,RECORDID=RECORDID,
                                             SOURCENAME=SOURCENAME,TABLECOLUMNS=TABLECOLUMNS,
                                             SOURCECOLUMN=SOURCECOLUMN,SOURCEKEY=SOURCEKEY,
                                             NULLCOLUMNS=NULLCOLUMNS,SGKKEY=SGKKEY,
                                             DATATYPE=DATATYPE,SGKID=SGKID,SOURCETABLE=SOURCETABLE,
                                             JOINRULE=JOINRULE,FILTERRULE=FILTERRULE,STGDB=STGDB,
                                             SOURCEDATABASE=SOURCEDATABASE
                                             )
        bteq_script = bteq_script.upper()
        f.write(bteq_script.replace('Â', ' '))
        f.close()
