from read_smx_sheet.app_Lib import functions as funcs
from read_smx_sheet.Logging_Decorator import Logging_decorator
from read_smx_sheet.parameters import parameters as pm
from os import path, makedirs
@Logging_decorator
def history_delete_insert_apply(cf, source_output_path, secondary_output_path_HIST, smx_table):
    hist_load_types = funcs.get_history_load_types(smx_table)
    folder_name = 'Apply_History_Delete_Insert'
    apply_folder_path = path.join(source_output_path, folder_name)
    makedirs(apply_folder_path)

    template_path = cf.templates_path + "/" + pm.default_history_deleteInsert_apply_template_file_name
    template_smx_path = cf.smx_path + "/" + "Templates" + "/" + pm.default_history_deleteInsert_apply_template_file_name
    LD_SCHEMA_NAME = cf.ld_prefix
    MODEL_SCHEMA_NAME = cf.modelDB_prefix
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

    history_handeled_df = funcs.get_apply_processes(smx_table, "APPLY_HISTORY_DELETE_INSERT")

    record_ids_list = history_handeled_df['Record_ID'].unique()

    for r_id in record_ids_list:
        history_df = funcs.get_sama_fsdm_record_id(history_handeled_df, r_id)

        record_id = r_id
        table_name = history_df['Entity'].unique()[0]
        source_name = history_df['Stg_Schema'].unique()[0]
        filename = table_name + '_R' + str(record_id)
        BTEQ_file_name = "UDI_{}_{}".format(source_name, filename)

        # f = funcs.WriteFile(apply_folder_path, BTEQ_file_name, "bteq")
        special_handling_flag = history_df['SPECIAL_HANDLING_FLAG'].unique()[0]
        if special_handling_flag.upper() == "N":
            f = funcs.WriteFile(apply_folder_path, BTEQ_file_name, "bteq")
        else:
            f = funcs.WriteFile(secondary_output_path_HIST, BTEQ_file_name, "bteq")


        tbl_columns = funcs.get_sama_table_columns_comma_separated(history_df, table_name, None, record_id)


        bteq_script = template_string.format(source_system=source_name,
                                              table_name=table_name,
                                              record_id=record_id,
                                              currentdate=current_date,
                                              bteq_run_file=bteq_run_file,
                                              ld_Schema_name=LD_SCHEMA_NAME,
                                              model_schema_name=MODEL_SCHEMA_NAME,
                                              table_columns=tbl_columns)
        bteq_script = bteq_script.upper()
        f.write(bteq_script.replace('Â', ' '))
        f.close()
