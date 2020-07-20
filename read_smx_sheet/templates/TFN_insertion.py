from read_smx_sheet.app_Lib import functions as funcs
from read_smx_sheet.Logging_Decorator import Logging_decorator
from read_smx_sheet.parameters import parameters as pm
from datetime import date

@Logging_decorator
def TFN_insertion(cf, source_output_path, SMX_SHEET):
    print('')
    template_path = cf.templates_path + "/" + pm.default_TFN_template_file_name
    template_smx_path = cf.smx_path + "/" + "Templates" + "/" + pm.default_TFN_template_file_name
    bteq_run_file = cf.bteq_run_file
    ld_prefix = cf.ld_prefix
    STG_prefix = cf.stg_prefix
    Source_name = cf.sgk_source
    if Source_name != 'ALL':
        SMX_SHEET = SMX_SHEET[SMX_SHEET['Ssource'] == Source_name]

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

    for record_id in record_ids_list:
        TFN_record_id_df = funcs.get_sama_fsdm_record_id(SMX_SHEET, record_id)
        Record_id = record_id

        fsdm_table_name = TFN_record_id_df['Entity'].unique()[0]
        ld_table_name = fsdm_table_name + "_R" + str(Record_id)

        ld_tbl_columns = funcs.get_fsdm_tbl_columns(TFN_record_id_df, alias_name=None)



