from read_smx_sheet.app_Lib import functions as funcs
from read_smx_sheet.Logging_Decorator import Logging_decorator
from read_smx_sheet.parameters import parameters as pm
from os import path, makedirs

@Logging_decorator
def history_legacy_apply(cf, source_output_path, secondary_output_path_HIST, smx_table):
    folder_name = 'Apply_History_LEGACY'
    apply_folder_path = path.join(source_output_path, folder_name)
    makedirs(apply_folder_path)

    template_path = cf.templates_path + "/" + pm.default_history_legacy_apply_template_file_name
    template_smx_path = cf.smx_path + "/" + "Templates" + "/" + pm.default_history_legacy_apply_template_file_name

    ld_schema_name = cf.ld_prefix
    model_Schema_name = cf.modelDB_prefix
    model_dup_Schema_name = cf.modelDup_prefix
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

    history_handeled_df = funcs.get_apply_processes(smx_table, "Apply_History_Legacy")

    record_ids_list = history_handeled_df['Record_ID'].unique()

    for r_id in record_ids_list:
        history_df = funcs.get_sama_fsdm_record_id(history_handeled_df, r_id)

        record_id = r_id
        table_name = history_df['Entity'].unique()[0]
        source_name = history_df['Stg_Schema'].unique()[0]
        filename = table_name + '_R' + str(record_id)
        BTEQ_file_name = "UDI_{}_{}".format(source_name, filename)

        special_handling_flag = history_df['SPECIAL_HANDLING_FLAG'].unique()[0]
        if special_handling_flag.upper() == "N":
            f = funcs.WriteFile(apply_folder_path, BTEQ_file_name, "bteq")
        else:
            f = funcs.WriteFile(secondary_output_path_HIST, BTEQ_file_name, "bteq")

        fsdm_tbl_alias = funcs.get_fsdm_tbl_alias(table_name)
        ld_tbl_alias = funcs.get_ld_tbl_alias(fsdm_tbl_alias, record_id)
        fsdm_tbl_alias = fsdm_tbl_alias + "_FSDM"
        strt_date, end_date, hist_keys, hist_cols = funcs.get_history_variables(history_df, record_id, table_name)

        strt_date = strt_date[0]
        end_date = end_date[0]
        # hist_cols = hist_cols[0]

        history_keys_list = funcs.get_list_values_comma_separated(hist_keys,'N')
        history_keys_columns = funcs.get_list_values_comma_separated(hist_keys, 'Y')
        history_columns = funcs.get_list_values_comma_separated(hist_cols, 'Y')
        #history_columns_list = funcs.get_list_values_comma_separated(hist_cols, 'N')
        max_history_columns_clause, pre_hist_cols_null_clause, hh_tbl_pre_not_eql_hh_tbl_hist_col = \
            funcs.get_hist_legacy_hist_cols_clauses(hist_cols, history_keys_list, strt_date, table_name)


        TBL_COLUMNS = funcs.get_sama_table_columns_comma_separated(history_df, table_name, None, record_id)

        HH_alias_TBL_COLUMNS = \
            funcs.get_sama_table_columns_comma_separated(history_df, table_name, 'HH_DATA', record_id)
        LRD_alias_TBL_COLUMNS = \
            funcs.get_sama_table_columns_comma_separated(history_df, table_name, ld_tbl_alias, record_id)
        FSDM_alias_TBL_COLUMNS = \
            funcs.get_sama_table_columns_comma_separated(history_df, table_name, fsdm_tbl_alias,
                                                                             record_id)


        ld_fsdm_history_key_and_strt_date_equality = funcs.get_conditional_stamenet(history_df, table_name,
                                                                                   'hist_key_strt_date', '=',
                                                                                   ld_tbl_alias, fsdm_tbl_alias,
                                                                                   record_id, None)

        interval = funcs.get_hist_end_Date_interval(history_df, table_name, record_id)

        high_date, end_date_dtype = funcs.get_hist_high_date(history_df, table_name, record_id)

        end_dt_coalesce_stmnt = "COALESCE(MAX({start_date} - {time_interval}) OVER (PARTITION BY  {history_keys_list} ORDER BY  {start_date} ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING), CAST('{high_date}' AS {end_date_dtype}))  {end_date}".format(start_date=strt_date,
                                                                                         time_interval=interval,
                                                                                         history_keys_list=history_keys_list,
                                                                                         high_date=high_date,
                                                                                         end_date_dtype=end_date_dtype,
                                                                                         end_date=end_date)

        TBL_COLS_EndDt_Coalesced = TBL_COLUMNS.replace(end_date, end_dt_coalesce_stmnt)

        bteq_script = template_string.format(source_system=source_name, versionnumber=pm.ver_no,
                                             currentdate=current_date,
                                             bteq_run_file=bteq_run_file,
                                             ld_schema_name=ld_schema_name,
                                             table_name=table_name,
                                             record_id=record_id,
                                             table_columns=TBL_COLUMNS,

                                             HH_aliased_table_columns=HH_alias_TBL_COLUMNS,
                                             LRD_aliased_table_columns=LRD_alias_TBL_COLUMNS,
                                             FSDM_aliased_table_columns=FSDM_alias_TBL_COLUMNS,

                                             ld_alias=ld_tbl_alias,
                                             model_schema_name=model_Schema_name,
                                             fsdm_alias=fsdm_tbl_alias,

                                             tbl_cols_minus_enddt=TBL_COLS_EndDt_Coalesced,
                                             ld_fsdm_history_key_and_strt_date_equality=ld_fsdm_history_key_and_strt_date_equality,

                                             history_keys_list=history_keys_list,
                                             history_keys_columns=history_keys_columns,

                                             max_history_columns_clause=max_history_columns_clause,
                                             pre_hist_cols_null=pre_hist_cols_null_clause,
                                             hh_tbl_pre_not_eql_hh_tbl_hist_col=hh_tbl_pre_not_eql_hh_tbl_hist_col,
                                             history_columns=history_columns,
                                             start_date=strt_date,
                                             end_date=end_date,
                                             time_interval=interval,
                                             high_date=high_date,
                                             end_date_dtype=end_date_dtype
                                              )
        bteq_script = bteq_script.upper()
        f.write(bteq_script.replace('Â', ' ').replace('\t', '    '))
        f.close()