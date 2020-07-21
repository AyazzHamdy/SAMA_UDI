from read_smx_sheet.app_Lib import functions as funcs
from read_smx_sheet.Logging_Decorator import Logging_decorator
from read_smx_sheet.parameters import parameters as pm
from os import path, makedirs
from datetime import date

@Logging_decorator
def apply_insert_upsert(cf, source_output_path, SMX_SHEET, script_flag):

        current_date = funcs.get_current_date()
        ld_prefix = cf.ld_prefix
        FSDM_prefix = cf.modelDB_prefix
        DupDB_prefix = cf.modelDup_prefix
        bteq_run_file = cf.bteq_run_file

        SOURCENAME = cf.sgk_source
        if SOURCENAME != 'ALL':
            SMX_SHEET = SMX_SHEET[SMX_SHEET['Ssource'] == SOURCENAME]


        if script_flag == 'Apply_Insert':
            SMX_SHEET = funcs.get_apply_processes(SMX_SHEET, "Apply_Insert")
            folder_name = 'Apply_Insert'
            template_path = cf.templates_path + "/" + pm.default_bteq_apply_insert_template_file_name
            template_smx_path = cf.smx_path + "/" + pm.default_bteq_apply_insert_template_file_name

        else:
            SMX_SHEET = funcs.get_apply_processes(SMX_SHEET, "Apply_Upsert")
            folder_name = 'Apply_Upsert'
            template_path = cf.templates_path + "/" + pm.default_bteq_apply_upsert_template_file_name
            template_smx_path = cf.smx_path + "/" + pm.default_bteq_apply_upsert_template_file_name

        apply_folder_path = path.join(source_output_path, folder_name)
        makedirs(apply_folder_path)

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


        # f.write(template_head)
        record_ids_list = SMX_SHEET['Record_ID'].unique()
        # print("****", script_flag,"........rec ids ......",record_ids_list )
        for record_id in record_ids_list:
            smx_record_id_df = funcs.get_sama_fsdm_record_id(SMX_SHEET, record_id)

            # source_system = funcs.get_Rid_Source_System(smx_record_id_df)
            # source_system = source_system.replace('Mobile Payments - ', '')
            Record_id = record_id
            schema_name = SOURCENAME
            # ld_DB = ld_prefix+schema_name

            Table_name = smx_record_id_df['Entity'].unique()[0]
            fsdm_tbl_alias = funcs.get_fsdm_tbl_alias(Table_name)

            ld_tbl_alias = funcs.get_ld_tbl_alias(fsdm_tbl_alias, Record_id)
            fsdm_tbl_alias = fsdm_tbl_alias + "_FSDM"
            ld_table_name = Table_name + "_R" + str(Record_id)
            BTEQ_file_name = "UDI_{}_{}".format(SOURCENAME, ld_table_name)

            f = funcs.WriteFile(apply_folder_path, BTEQ_file_name, "bteq")
            f.write(template_head)

            # ld_tbl_columns_aliased = funcs.get_fsdm_tbl_non_technical_columns(smx_record_id_df, ld_tbl_alias)
            ld_pk_cols_aliased = funcs.get_sama_pk_columns_comma_separated(smx_record_id_df, Table_name, alias=ld_tbl_alias, record_id=Record_id)

            fsdm_tbl_columns = funcs.get_fsdm_tbl_columns(smx_record_id_df, alias_name=None)

            on_clause = funcs.get_conditional_stamenet(smx_record_id_df, Table_name, "pk", "=", ld_tbl_alias, fsdm_tbl_alias, Record_id)

            where_clause = funcs.get_conditional_stamenet(smx_record_id_df, Table_name, "pk", "=", ld_tbl_alias, "FLAG_IND", Record_id)

            FSDM_tbl_pk= funcs.get_sama_pk_columns_comma_separated(smx_record_id_df, Table_name, alias=fsdm_tbl_alias, record_id=Record_id)
            FSDM_first_tbl_pk = FSDM_tbl_pk.split(',')[0]

            COALESCED_TABLE_nonPK_COLUMNS_LD_EQL_FSDM = funcs.get_comparison_columns(smx_record_id_df, Table_name,
                                                                                     "UPSERT", '=', ld_tbl_alias,
                                                                                     fsdm_tbl_alias, Record_id)

            if script_flag == 'Apply_Insert':
                # COALESCED_TABLE_PK_COLUMNS_LD_EQL_FSDM = funcs.get_comparison_columns(smx_record_id_df, Table_name, "INSERT"
                #                                                                       , '=', ld_tbl_alias, fsdm_tbl_alias,
                #                                                                       Record_id)
                bteq_script = template_string.format(filename=BTEQ_file_name,#versionnumber=pm.ver_no,
                                                     currentdate=current_date,
                                                     bteq_run_file=bteq_run_file,
                                                     ld_prefix=ld_prefix,
                                                     schema_name=schema_name,
                                                     ld_table_name=ld_table_name,
                                                     tbl_pk_cols_aliased=ld_pk_cols_aliased,
                                                     ld_tbl_alias=ld_tbl_alias,
                                                     fsdm_tbl_alias=fsdm_tbl_alias,
                                                     table_columns=fsdm_tbl_columns,
                                                     FSDM_first_tbl_pk=FSDM_first_tbl_pk.strip(),
                                                     COALESCED_TABLE_nonPK_COLUMNS_LD_EQL_FSDM=COALESCED_TABLE_nonPK_COLUMNS_LD_EQL_FSDM,
                                                     fsdm_prefix=FSDM_prefix,
                                                     fsdm_table_name=Table_name,
                                                     ld_equal_fsdm_pk=on_clause,
                                                     FLAG_IND_equal_fsdm_pk=where_clause,
                                                     dup_prefix=DupDB_prefix
                                                     )
            else:

                ld_equal_fsdm_pk_update = funcs.get_conditional_stamenet(smx_record_id_df, Table_name, "pk", "=",
                                                                         fsdm_tbl_alias, ld_tbl_alias,
                                                                         Record_id)
                non_pk_cols_eql_ld_cols = funcs.get_conditional_stamenet(smx_record_id_df, Table_name, "non_pk_upsert_set",
                                                                         "=", None, ld_tbl_alias, Record_id)
                non_pk_cols_eql_ld_cols = non_pk_cols_eql_ld_cols.replace(' and ', ',')

                bteq_script = template_string.format(filename=BTEQ_file_name, #versionnumber=pm.ver_no,
                                                     currentdate=current_date,
                                                     bteq_run_file=bteq_run_file,
                                                     ld_prefix=ld_prefix,
                                                     schema_name=schema_name,
                                                     ld_table_name=ld_table_name,
                                                     tbl_pk_cols_aliased=ld_pk_cols_aliased,
                                                     ld_tbl_alias=ld_tbl_alias,
                                                     fsdm_tbl_alias=fsdm_tbl_alias,
                                                     table_columns=fsdm_tbl_columns,
                                                     FSDM_first_tbl_pk=FSDM_first_tbl_pk.strip(),
                                                     COALESCED_TABLE_nonPK_COLUMNS_LD_EQL_FSDM=COALESCED_TABLE_nonPK_COLUMNS_LD_EQL_FSDM,
                                                     non_pk_cols_eql_ld_cols=non_pk_cols_eql_ld_cols,
                                                     fsdm_prefix=FSDM_prefix,
                                                     fsdm_table_name=Table_name,
                                                     ld_equal_fsdm_pk=on_clause,
                                                     FLAG_IND_equal_fsdm_pk=where_clause,
                                                     ld_equal_fsdm_pk_update=ld_equal_fsdm_pk_update,
                                                     dup_prefix=DupDB_prefix
                                                     )

            bteq_script = bteq_script.upper()
            f.write(bteq_script.replace('Â', ' '))
            f.close()

