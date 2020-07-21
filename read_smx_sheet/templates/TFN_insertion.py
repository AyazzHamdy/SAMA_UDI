from read_smx_sheet.app_Lib import functions as funcs
from read_smx_sheet.Logging_Decorator import Logging_decorator
from read_smx_sheet.parameters import parameters as pm
from datetime import date

@Logging_decorator
def TFN_insertion(cf, source_output_path, SMX_SHEET):

    template_path = cf.templates_path + "/" + pm.default_TFN_template_file_name
    template_smx_path = cf.smx_path + "/" + "Templates" + "/" + pm.default_TFN_template_file_name

    bteq_run_file = cf.bteq_run_file
    ld_prefix = cf.ld_prefix
    STG_prefix = cf.stg_prefix

    current_date = funcs.get_current_date()
    Source_name = cf.sgk_source
    if Source_name != 'ALL':
        SMX_SHEET = SMX_SHEET[SMX_SHEET['Ssource'] == Source_name]

    SMX_SHEET = funcs.get_apply_processes(SMX_SHEET, "TFN")

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
                template_head = template_head + i
                template_head_line = template_head_line + 1
            else:
                template_string = template_string + i
                template_start = template_head_line + 1

    record_ids_list = SMX_SHEET['Record_ID'].unique()
    bteq_script = ""

    for record_id in record_ids_list:
        TFN_record_id_df = funcs.get_sama_fsdm_record_id(SMX_SHEET, record_id)

        Record_id = record_id

        fsdm_table_name = TFN_record_id_df['Entity'].unique()[0]
        ld_table_name = fsdm_table_name + "_R" + str(Record_id)
        BTEQ_file_name = "{}_{}_R{}".format(Source_name, fsdm_table_name, Record_id)
        f = funcs.WriteFile(source_output_path, BTEQ_file_name, "bteq")
        f.write(template_head)

        ld_tbl_columns = funcs.get_fsdm_tbl_columns(TFN_record_id_df, alias_name=None)
        src_table = funcs.get_Rid_Source_Table(TFN_record_id_df)

        col_mapping = funcs.get_TFN_column_mapping(TFN_record_id_df)
        left_joins = funcs.rule_col_analysis_sgk(TFN_record_id_df)

        bteq_script = template_string.format(currentdate=current_date,
                                             bteq_run_file=bteq_run_file,
                                             ld_prefix=ld_prefix,
                                             FSDM_tbl_Name=fsdm_table_name,
                                             Source_name=Source_name,
                                             Record_Id=Record_id,
                                             ld_tbl_name=ld_table_name,
                                             ld_tbl_cols=ld_tbl_columns,
                                             Column_Mapping=col_mapping,
                                             STG_prefix=STG_prefix,
                                             STG_tbl=src_table,
                                             possible_left_joins=left_joins
                                             )

        bteq_script = bteq_script.upper()
        f.write(bteq_script.replace('Â', ' '))
        f.close()
