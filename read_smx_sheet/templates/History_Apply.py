from read_smx_sheet.app_Lib import functions as funcs
from read_smx_sheet.Logging_Decorator import Logging_decorator
from read_smx_sheet.parameters import parameters as pm
from os import path, makedirs
import pandas as pd

@Logging_decorator
def history_apply(cf, source_output_path, secondary_output_path_HIST, smx_table, pure_history_flag):

    template_path = cf.templates_path + "/" + pm.default_history_apply_template_file_name
    template_smx_path = cf.smx_path + "/" + "Templates" + "/" + pm.default_history_apply_template_file_name
    LD_SCHEMA_NAME = cf.ld_prefix
    MODEL_SCHEMA_NAME = cf.modelDB_prefix
    MODEL_DUP_SCHEMA_NAME = cf.modelDup_prefix
    bteq_run_file = cf.bteq_run_file
    current_date = funcs.get_current_date()

    template_string = ""
    try:
        template_file = open(template_path, "r")
    except:
        template_file = open(template_smx_path, "r")

    for i in template_file.readlines():
        if i != "":
            template_string = template_string + i

    if pure_history_flag is True:
        folder_name = 'Apply_History'
        apply_folder_path = path.join(source_output_path, folder_name)
        makedirs(apply_folder_path)

        history_handeled_df = funcs.get_apply_processes(smx_table, "Apply_History")
    else:
        folder_name = 'Apply_History_LEGACY'
        subfolder_name = 'SUBSEQUENT_LOADS'
        apply_folder_path = path.join(source_output_path, folder_name, subfolder_name)
        if not path.exists(apply_folder_path):
            makedirs(apply_folder_path)
        history_handeled_df = smx_table

    record_ids_list = history_handeled_df['Record_ID'].unique()


    for r_id in record_ids_list:
        history_df = funcs.get_sama_fsdm_record_id(history_handeled_df, r_id)

        record_id = r_id
        table_name = history_df['Entity'].unique()[0]

        SOURCENAME = history_df['Stg_Schema'].unique()[0]
        filename = table_name + '_R' + str(record_id)
        BTEQ_file_name = "UDI_{}_{}".format(SOURCENAME, filename)

        # f = funcs.WriteFile(apply_folder_path, BTEQ_file_name, "bteq")
        special_handling_flag = history_df['SPECIAL_HANDLING_FLAG'].unique()[0]
        if special_handling_flag.upper() == "N":
            f = funcs.WriteFile(apply_folder_path, BTEQ_file_name, "bteq")
        else:
            if pure_history_flag is False:  #not pure history but history legacy subsequent loads
                folder_name_Sapecial = 'Special_Apply_History_LEGACY/SUBSEQUENT_LOADS'
                apply_folder_path_Sapecial = path.join(secondary_output_path_HIST, folder_name_Sapecial)
                if not path.exists(apply_folder_path_Sapecial):
                    makedirs(apply_folder_path_Sapecial)
                f = funcs.WriteFile(apply_folder_path_Sapecial, BTEQ_file_name, "bteq")
            else:  #pure history
                folder_name_Sapecial = 'Apply_History'
                apply_folder_path_Sapecial = path.join(secondary_output_path_HIST, folder_name_Sapecial)
                if not path.exists(apply_folder_path_Sapecial):
                    makedirs(apply_folder_path_Sapecial)
                f = funcs.WriteFile(apply_folder_path_Sapecial, BTEQ_file_name, "bteq")

        filename = filename + '.bteq'

        fsdm_tbl_alias = funcs.get_fsdm_tbl_alias(table_name)
        ld_tbl_alias = funcs.get_ld_tbl_alias(fsdm_tbl_alias, record_id)
        fsdm_tbl_alias = fsdm_tbl_alias+"_FSDM"

        strt_date, end_date, hist_keys, hist_cols = funcs.get_history_variables(history_df, record_id, table_name)

        first_history_key = hist_keys[0]
        strt_date = strt_date[0]
        end_date = end_date[0]


        hist_keys_aliased = funcs.get_aliased_columns(hist_keys, ld_tbl_alias)
        COALESCED_history_col_LD_EQL_DATAMODEL = funcs.get_comparison_columns(history_df, table_name,
                                                                              "HISTORY_COL", '=', ld_tbl_alias,
                                                                              fsdm_tbl_alias, record_id)
        # print("COALESCED_history_col_LD_EQL_DATAMODEL", COALESCED_history_col_LD_EQL_DATAMODEL)
        if COALESCED_history_col_LD_EQL_DATAMODEL.strip() == "":
            COALESCED_history_col_LD_EQL_DATAMODEL = "/* This is a special history case, please refer to the" \
                                                     " SMX's Rules column to deduce the needed History_columns " \
                                                     "that will be changing */"

        ld_fsdm_history_key_and_end_date_equality = funcs.get_conditional_stamenet(history_df, table_name,
                                                                                   'hist_key_end_date', '=',
                                                                                   ld_tbl_alias, fsdm_tbl_alias,
                                                                                   record_id, None)

        ld_fsdm_history_key_and_strt_date_equality = funcs.get_conditional_stamenet(history_df, table_name,
                                                                                    'hist_key_strt_date', '=',
                                                                                    ld_tbl_alias, "FLAG_IND",
                                                                                    record_id, None)


        # end_date_updt = funcs.get_hist_end_dt_updtt(history_df, table_name, end_date, "=", None,ld_tbl_alias, record_id)

        possible_special_handling_comments = ""
        if special_handling_flag.upper() == "Y":
            possible_special_handling_comments = history_df[history_df['Historization_Column'].str.upper() == 'E']['Rule'].values
            possible_special_handling_comments = "/*" + str(possible_special_handling_comments).replace("\n", " ") + "*/"

        TBL_COLUMNS = funcs.get_sama_table_columns_comma_separated(history_df, table_name, None, record_id)

        interval = funcs.get_hist_end_Date_interval(history_df, table_name, record_id)


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
                                             # end_date_updt=end_date_updt,
                                             end_date=end_date,
                                             interval=interval,
                                             possible_special_handling_comments=possible_special_handling_comments
                                             )
        bteq_script = bteq_script.upper()
        f.write(bteq_script.replace('Â', ' '))
        f.close()
