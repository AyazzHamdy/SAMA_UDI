from read_smx_sheet.app_Lib import functions as funcs
from read_smx_sheet.Logging_Decorator import Logging_decorator
from read_smx_sheet.parameters import parameters as pm
from datetime import date
from os import path, makedirs

@Logging_decorator
def history_apply(cf, source_output_path, smx_table):

    hist_load_types = funcs.get_history_load_types(smx_table)
    folder_name = 'Apply_History'
    apply_folder_path = path.join(source_output_path, folder_name)
    makedirs(apply_folder_path)

    template_path = cf.templates_path + "/" + pm.default_history_apply_template_file_name
    template_smx_path = cf.smx_path + "/" + "Templates" + "/" + pm.default_history_apply_template_file_name
    LD_SCHEMA_NAME = cf.ld_prefix
    MODEL_SCHEMA_NAME = cf.modelDB_prefix
    MODEL_DUP_SCHEMA_NAME = cf.modelDup_prefix
    bteq_run_file = cf.bteq_run_file
    current_date = funcs.get_current_date()

    SOURCENAME = cf.sgk_source
    if SOURCENAME != 'ALL':
        smx_table = smx_table[smx_table['Ssource'] == SOURCENAME]

    template_string = ""
    try:
        template_file = open(template_path, "r")
    except:
        template_file = open(template_smx_path, "r")

    for i in template_file.readlines():
        if i != "":
            template_string = template_string + i

    history_handeled_df = funcs.get_history_handled_processes(smx_table, hist_load_types)

    record_ids_list = history_handeled_df['Record_ID'].unique()
    print ("record_ids_list", record_ids_list)
    for r_id in record_ids_list:
        history_df = funcs.get_sama_fsdm_record_id(history_handeled_df, r_id)

        record_id = r_id
        table_name = history_handeled_df['Entity'].unique()[0]
        filename = table_name + '_' + str(record_id)
        f = funcs.WriteFile(apply_folder_path, filename, "bteq")
        filename = filename + '.bteq'

        fsdm_tbl_alias = funcs.get_fsdm_tbl_alias(table_name)
        ld_tbl_alias = funcs.get_ld_tbl_alias(fsdm_tbl_alias, record_id)
        fsdm_tbl_alias = fsdm_tbl_alias+"_FSDM"
        strt_date, end_date, hist_keys, hist_cols = funcs.get_history_variables(history_handeled_df, record_id, table_name)
        print("hist_keys, hist_keys", hist_keys)
        print("hist_cols, hist_cols", hist_cols)
        first_history_key = hist_keys[0]
        strt_date = strt_date[0]
        end_date = end_date[0]

        hist_keys_aliased = funcs.get_aliased_columns(hist_keys, ld_tbl_alias)
        COALESCED_history_col_LD_EQL_DATAMODEL = funcs.get_comparison_columns(history_handeled_df, table_name,
                                                                              "HISTORY_COL", '=', ld_tbl_alias,
                                                                              fsdm_tbl_alias, record_id)

        ld_fsdm_history_key_and_end_date_equality = funcs.get_conditional_stamenet(history_handeled_df, table_name,
                                                                                   'hist_key_end_date', '=',
                                                                                   ld_tbl_alias, fsdm_tbl_alias,
                                                                                   record_id, None)

        ld_fsdm_history_key_and_strt_date_equality = funcs.get_conditional_stamenet(history_handeled_df, table_name,
                                                                                    'hist_key_strt_date', '=',
                                                                                    ld_tbl_alias, "FLAG_IND",
                                                                                    record_id, None)

        end_date_updt = funcs.get_hist_end_dt_updt(end_date, "end_date", "=", None, ld_tbl_alias, record_id)

        TBL_COLUMNS = funcs.get_sama_table_columns_comma_separated(history_handeled_df, table_name, None, record_id)

        bteq_script = template_string.format(SOURCE_SYSTEM=SOURCENAME, versionnumber=pm.ver_no,
                                             currentdate=current_date,
                                             filename=filename,
                                             bteq_run_file=bteq_run_file, LD_SCHEMA_NAME=LD_SCHEMA_NAME,
                                             MODEL_SCHEMA_NAME=MODEL_SCHEMA_NAME,
                                             TABLE_COLUMNS=TBL_COLUMNS,
                                             MODEL_DUP_SCHEMA_NAME=MODEL_DUP_SCHEMA_NAME,
                                             TABLE_NAME=table_name, RECORD_ID=record_id,
                                             ld_alias=ld_tbl_alias, fsdm_alias=fsdm_tbl_alias,
                                             history_key=hist_keys_aliased,
                                             start_date=strt_date, first_history_key=first_history_key,
                                             COALESCED_history_col_LD_EQL_DATAMODEL=COALESCED_history_col_LD_EQL_DATAMODEL,
                                             ld_fsdm_history_key_and_end_date_equality=ld_fsdm_history_key_and_end_date_equality,
                                             ld_fsdm_history_key_and_strt_date_equality=ld_fsdm_history_key_and_strt_date_equality,
                                             end_date_updt=end_date_updt
                                             )
        bteq_script = bteq_script.upper()
        f.write(bteq_script.replace('Â', ' '))
        f.close()
