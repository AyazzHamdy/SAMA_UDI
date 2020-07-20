from read_smx_sheet.app_Lib import functions as funcs
from read_smx_sheet.Logging_Decorator import Logging_decorator
from read_smx_sheet.parameters import parameters as pm
from datetime import date
from os import path, makedirs

@Logging_decorator
def history_apply(cf, source_output_path, smx_table):
    load_types_list = smx_table['Load Type'].unique()
    hist_load_types = []

    for i in range(len(load_types_list)):
        if 'History'.upper() in load_types_list[i].upper():
            hist_load_types.append(load_types_list[i])

    folder_name = 'Apply_History'
    apply_folder_path = path.join(source_output_path, folder_name)
    makedirs(apply_folder_path)

    template_path = cf.templates_path + "/" + pm.default_history_apply_template_file_name
    template_smx_path = cf.smx_path + "/" + "Templates" + "/" + pm.default_history_apply_template_file_name
    LD_SCHEMA_NAME = cf.ld_prefix
    MODEL_SCHEMA_NAME = cf.modelDB_prefix
    MODEL_DUP_SCHEMA_NAME = cf.modelDup_prefix
    bteq_run_file = cf.bteq_run_file
    today = date.today()
    today = today.strftime("%d/%m/%Y")
    template_string = ""
    try:
        template_file = open(template_path, "r")
    except:
        template_file = open(template_smx_path, "r")

    for i in template_file.readlines():
        if i != "":
            template_string = template_string + i

    history_handeled_df = funcs.get_history_handled_processes(smx_table, hist_load_types)


    for history_df_index, history_df_row in history_handeled_df.iterrows():
        record_id = history_df_row['Record_ID']
        table_name = history_df_row['Entity']
        SOURCE_SYSTEM = history_df_row['Source_System']
        filename = table_name + '_' + str(record_id)



        f = funcs.WriteFile(apply_folder_path, filename, "bteq")
        filename = filename + '.bteq'
        PK_TABLE_COLOUMNS_WITH_ALIAS_LD = funcs.get_sama_pk_columns_comma_separated(history_handeled_df, table_name,
                                                                                    'LOAD_TABLE', record_id)
        TABLE_PK = funcs.get_sama_pk_columns_comma_separated(history_handeled_df, table_name, 'one_pk', record_id)
        COALESCED_TABLE_COLUMNS_LD_EQL_DATAMODEL = funcs.get_comparison_columns(history_handeled_df, table_name,
                                                                                "History", '=', 'LOAD_TABLE',
                                                                                'MODEL_TABLE', record_id)
        LOADTBL_PK_EQL_MODELTBL = funcs.get_conditional_stamenet(history_handeled_df, table_name, 'pk', '=',
                                                                 'LOAD_TABLE', 'MODEL_TABLE', record_id, 'histort')
        LOADTBL_PK_EQL_FLAGIND = funcs.get_conditional_stamenet(history_handeled_df, table_name, 'pk', '=',
                                                                'LOAD_TABLE', 'FLAG_IND', record_id, 'history')
        TABLE_COLUMNS = funcs.get_sama_table_columns_comma_separated(history_handeled_df, table_name, None, record_id)
        NON_PK_COLS_EQL_LD = funcs.get_conditional_stamenet(history_handeled_df, table_name, 'non_pk', '=',
                                                            'MODEL_TABLE', 'LOAD_TABLE', record_id)

        bteq_script = template_string.format(SOURCE_SYSTEM=SOURCE_SYSTEM,versionnumber=pm.ver_no,
                                             currentdate=today,
                                             filename=filename,
                                             bteq_run_file=bteq_run_file, LD_SCHEMA_NAME=LD_SCHEMA_NAME,
                                             MODEL_SCHEMA_NAME=MODEL_SCHEMA_NAME,
                                             MODEL_DUP_SCHEMA_NAME=MODEL_DUP_SCHEMA_NAME,
                                             TABLE_NAME=table_name, RECORD_ID=record_id,
                                             PK_TABLE_COLOUMNS_WITH_ALIAS_LD=PK_TABLE_COLOUMNS_WITH_ALIAS_LD,
                                             TABLE_PK=TABLE_PK,
                                             COALESCED_TABLE_COLUMNS_LD_EQL_DATAMODEL=COALESCED_TABLE_COLUMNS_LD_EQL_DATAMODEL,
                                             LOADTBL_PK_EQL_MODELTBL=LOADTBL_PK_EQL_MODELTBL,
                                             LOADTBL_PK_EQL_FLAGIND=LOADTBL_PK_EQL_FLAGIND,
                                             NON_PK_COLS_EQL_LD=NON_PK_COLS_EQL_LD,
                                             TABLE_COLUMNS=TABLE_COLUMNS
                                             )
        bteq_script = bteq_script.upper()
        f.write(bteq_script)
        f.close()
