default_config_file_name = "config.txt"
parquet_db_name = "smx_data"
sys_argv_separator = "|#|"
stg_cols_separator = "||'_'||"
oi_staging_template_file_name = "OI_staging_template.txt"
log_staging_template_file_name = "LOG_staging_template.txt"
uv_staging_template_file_name = "UV_staging_template.txt"
default_data_mart_template_file_name = "Data_mart_template.txt"
default_staging_template_file_name = "Staging_template.txt"
default_bteq_stg_datamart_template_file_name = "BTEQ_stg_datamart_template.txt"
default_bteq_stg_datamart_leftjoin_template_file_name= "BTEQ_stg_datamart_leftjoin_template.txt"
default_bteq_oi_stg_template_file_name = "BTEQ_oi_stg_template.txt"
default_history_apply_template_file_name = "History_template.txt"
default_history_legacy_apply_template_file_name = 'BTEQ_history_legacy_template.txt'
default_history_deleteInsert_apply_template_file_name = 'BTEQ_history_del_insert_template.txt'
default_bteq_apply_insert_template_file_name = "BTEQ_apply_insert_template.txt"
default_bteq_apply_upsert_template_file_name = "BTEQ_apply_upsert_template.txt"
default_bteq_apply_delete_insert_template_file_name = "BTEQ_delete_insert_template.txt"
default_sgk_template_file_name = "SGK_template.txt"
default_TFN_template_file_name = "BTEQ_TFN_template.txt"
tfn_concat_template_name = "BTEQ_TFN_CONCAT.txt"
smx_ext = "xlsx"
System_sht = "System"
Supplements_sht = "Supplements"
STG_tables_sht = "STG tables"
Data_types_sht = "Data type"
smx_sht = "SMX"

staging_sheets = [Data_types_sht,STG_tables_sht]
smx_sheets = [smx_sht]

# AppName_<Major>.<Minor>.<BuildNo>
ver_no = "| Build #3.1.1"
# ################################################################################################