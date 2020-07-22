import os
import sys

sys.path.append(os.getcwd())
import numpy as np
import pandas as pd
# import pyarrow.parquet as pq
# import pyarrow as pa
# from pyarrow.formatting import *
import dask.dataframe as dd
from read_smx_sheet.app_Lib import manage_directories as md
from read_smx_sheet.parameters import parameters as pm
import datetime as dt
import psutil
from datetime import date


def read_excel(file_path, sheet_name, filter=None, reserved_words_validation=None, nan_to_empty=True):
    try:
        df = pd.read_excel(file_path, sheet_name, na_filter=False)
        df_cols = list(df.columns.values)
        df = df.applymap(lambda x: x.strip() if type(x) is str else x)

        if filter:
            df = df_filter(df, filter, False)

        if nan_to_empty:
            if isinstance(df, pd.DataFrame):
                df = replace_nan(df, '')
                df = df.applymap(lambda x: int(x) if type(x) is float else x)
            else:
                df = pd.DataFrame(columns=df_cols)

        if reserved_words_validation is not None:
            df = rename_sheet_reserved_word(df, reserved_words_validation[0], reserved_words_validation[1],
                                            reserved_words_validation[2])

    except:
        df = pd.DataFrame()
    return df


def df_filter(df, filter=None, filter_index=True):
    df_cols = list(df.columns.values)
    if filter:
        for i in filter:
            if filter_index:
                df = df[df.index.isin(i[1])]
            else:
                df = df[df[i[0]].isin(i[1])]

    if df.empty:
        df = pd.DataFrame(columns=df_cols)

    return df


def replace_nan(df, replace_with):
    return df.replace(np.nan, replace_with, regex=True)


def is_Reserved_word(Supplements, Reserved_words_source, word):
    Reserved_words = Supplements[Supplements['Reserved words source'] == Reserved_words_source][['Reserved words']]
    is_Reserved_word = True if Reserved_words[Reserved_words['Reserved words'] == word][
                                   'Reserved words'].any() == word else False
    return is_Reserved_word


def rename_sheet_reserved_word(sheet_df, Supplements_df, Reserved_words_source, columns):
    if not sheet_df.empty:
        for col in columns:
            sheet_df[col] = sheet_df.apply(
                lambda row: rename_reserved_word(Supplements_df, Reserved_words_source, row[col]), axis=1)
    return sheet_df


def rename_reserved_word(Supplements, Reserved_words_source, word):
    return word + '_' if is_Reserved_word(Supplements, Reserved_words_source, word) else word


def get_file_name(file):
    return os.path.splitext(os.path.basename(file))[0]

def get_history_load_types(smx_sheet_df):
    load_types_list = smx_sheet_df['Load Type'].unique()
    hist_load_types = []
    for i in range(len(load_types_list)):
        if 'History'.upper() in load_types_list[i].upper():
            hist_load_types.append(load_types_list[i])
    return hist_load_types

def get_insert_load_types(smx_sheet_df):
    load_types_list = smx_sheet_df['Load Type'].unique()
    insert_load_types = []
    for i in range(len(load_types_list)):
        if 'Insert'.upper() in load_types_list[i].upper():
            insert_load_types.append(load_types_list[i])
    # print("insert_load_types", insert_load_types)
    return insert_load_types

def get_upsert_load_types(smx_sheet_df):
    load_types_list = smx_sheet_df['Load Type'].unique()
    upsert_load_types = []
    for i in range(len(load_types_list)):
        if 'Upsert'.upper() in load_types_list[i].upper():
            upsert_load_types.append(load_types_list[i])
    # print("upsert_load_types", upsert_load_types)
    return upsert_load_types

def is_history_load_type(TFN_Rid_df):
    hist_load_types = get_history_load_types(TFN_Rid_df)
    if TFN_Rid_df[TFN_Rid_df['Load Type'].isin(hist_load_types)]:
        return True
    else:
        return False

def get_apply_processes(smx_sheet, apply_type):
    # print("get_apply_processes", apply_type.upper())
    if apply_type.upper() == "APPLY_INSERT":
        insrt_load_types = get_insert_load_types(smx_sheet)
        apply_tfns = smx_sheet.loc[smx_sheet['Load Type'].isin(insrt_load_types)]
        # print("insrt_load_types", insrt_load_types)
    elif apply_type.upper() == "APPLY_UPSERT":
        upsrt_load_types = get_upsert_load_types(smx_sheet)
        apply_tfns = smx_sheet.loc[smx_sheet['Load Type'].isin(upsrt_load_types)]
        # print("upsrt_load_types", upsrt_load_types)
    elif apply_type.upper() == "APPLY_HISTORY":
        hist_load_types = get_history_load_types(smx_sheet)
        apply_tfns = smx_sheet.loc[smx_sheet['Load Type'].isin(hist_load_types)]
    else:
        apply_tfns = smx_sheet


    # apply_processes = apply_tfns[(apply_tfns['Source_System'] != 'EMDAD_M')]
    emdad_Rids_list = get_EMDAD_Rids_list(smx_sheet)
    apply_processes = apply_tfns[~apply_tfns.Record_ID.isin(emdad_Rids_list)]
    apply_processes = apply_processes[~apply_processes['Entity'].str.endswith(str('_SGK'))]
    return apply_processes

def get_EMDAD_Rids_list(smx_sheet):
    emdad_df = smx_sheet[smx_sheet['Source_System'] == 'EMDAD_M']
    emdad_Rids_list = emdad_df['Record_ID'].unique()
    return emdad_Rids_list

def get_sama_stg_tables(STG_tables, source_name=None):
    if source_name:
        stg_table_names = STG_tables.loc[STG_tables['Source System'] == source_name][
            ['TABLE_NAME', 'SCHEMA_NAME']].drop_duplicates()
    else:
        stg_table_names = STG_tables[['TABLE_NAME', 'SCHEMA_NAME']].drop_duplicates()
    return stg_table_names


def get_sama_stg_table_columns(STG_tables, Table_name):
    STG_tables_df = STG_tables.loc[
        (STG_tables['TABLE_NAME'].str.upper() == Table_name.upper())
    ].reset_index()

    return STG_tables_df


def get_sama_fsdm_record_id(SMX_SHEET, R_id):
    smx_Rid = SMX_SHEET.loc[
        (SMX_SHEET['Record_ID'] == R_id)
    ].reset_index()
    return smx_Rid.drop_duplicates()

def get_Rid_Source_Table(SMX_R_id):
    tech_src_tbl_list = get_SMX_tech_Source_Table_vals()
    src_tbl_list = SMX_R_id['Source_Table'].unique()
    o_src_tbls_list = np.setdiff1d(src_tbl_list, tech_src_tbl_list).tolist()
    print("R_id: ", SMX_R_id["Record_ID"].unique(), "o_src_tbls_list: ", o_src_tbls_list)
    try:
        src_tbl = o_src_tbls_list[0]
    except:
        src_tbl = " "
    src_tbl_name = '"'+src_tbl+'"' if str(src_tbl[0]) == str(0) else src_tbl
    return src_tbl_name

def get_SMX_tech_Source_Table_vals():
    tech_vals_list = ['HCV', 'JOB']
    return tech_vals_list

def get_fsdm_tbl_alias(Table_name):
    alias_name = ' '
    tbl_name_elements = Table_name.split("_")
    for i in range(len(tbl_name_elements)):
        alias_name = str(alias_name) + str(tbl_name_elements[i][0])
    alias_name = alias_name.strip()
    return alias_name

def get_ld_tbl_alias(fsdm_tbl_alias, rid):
    ld_tbl_alias = "{}_R{}_LD".format(fsdm_tbl_alias, rid)
    return ld_tbl_alias.strip()

def get_TFN_rid_no_tech_cols(smx_Rid_df):  #remove the rows that have the tech cols from the df
    tech_cols = get_fsdm_tech_cols_list()
    TFN_df = smx_Rid_df[~smx_Rid_df.Column.isin(tech_cols)]
    return TFN_df

def get_TFN_column_mapping(smx_Rid_df):
    # tech_cols = get_fsdm_tech_cols_list()
    # TFN_df = smx_Rid_df[~smx_Rid_df.Column.isin(tech_cols)]
    TFN_df = get_TFN_rid_no_tech_cols(smx_Rid_df)
    columns_comma = ""
    stg_alias = "STG."
    sgk_alias = "SGK."
    for tfn_Rid_indx, tfn_Rid_row in TFN_df.iterrows():
        comma = '    ' + ',' if tfn_Rid_indx > 0 else ''
        col_name = tfn_Rid_row['Column'].upper()
        col_dtype = tfn_Rid_row['Datatype'].upper()
        col_dtype = handle_default_col_dtype(col_dtype)
        src_tbl = tfn_Rid_row['Source_Table'].upper()
        src_col = tfn_Rid_row['Source_Column'].upper()
        load_type = tfn_Rid_row['Load Type'].upper()

        rule = tfn_Rid_row['Rule']
        # print("rule", rule)
        rule = str(rule).replace("\n", " ")
        # print("rule2", rule)
        if src_tbl == 'HCV' and src_col == 'HCV':
            if rule == 'NULL':
                HCV = 'NULL'
            else:
                applied_rule = rule.replace("Hardcode to", " ").replace("HARDCODE TO", " ").strip()
                HCV = applied_rule
            column_clause = "CAST( {} AS {} ) AS {} /*{}*/".format(HCV, col_dtype, col_name, rule)
            columns_comma += comma + column_clause + '\n'

        elif rule == "1:1" and src_tbl != 'JOB':
            column_clause = "CAST( {}{} AS {} ) AS {} /*{}*/".format(stg_alias, src_col, col_dtype, col_name, rule)
            columns_comma += comma + column_clause + '\n'

        elif src_tbl == 'JOB' and "_STRT_" in col_name:
            HCV_strt = "CURRENT_{}".format(col_dtype)
            column_clause = "CAST( {} AS {} ) AS {} /*{}*/".format(HCV_strt, col_dtype, col_name, rule)
            columns_comma += comma + column_clause + '\n'
        elif "HISTORY" in load_type and src_tbl == 'JOB' and "_END_" in col_name:
            HCV_end = "9999-12-31 23:59:59.999999"
            HCV_end = single_quotes(HCV_end)
            column_clause = "CAST( {} AS {} ) AS {} /* {}*/".format(HCV_end, col_dtype, col_name, rule)
            columns_comma += comma + column_clause + '\n'
        else:
            column_clause = col_name + "/* mapped to {}.{} following rule: {}*/".format(src_tbl, src_col, rule)
            columns_comma += comma + column_clause + '\n'

    columns_comma = columns_comma[0:len(columns_comma) - 1]
    return columns_comma


def handle_default_col_dtype(dtype):
    if dtype == 'TIMESTAMP':
        o_dtype = 'TIMESTAMP(6)'
    else:
        o_dtype = dtype
    return o_dtype.upper()


def rule_col_analysis_sgk(smx_Rid_df):
    tech_cols = get_fsdm_tech_cols_list()
    TFN_df = smx_Rid_df[~smx_Rid_df.Column.isin(tech_cols)]
    left_joins = " "
    rule_output = " "
    sgk_cntr = 1
    for tfn_Rid_indx, tfn_Rid_row in TFN_df.iterrows():
        rule = tfn_Rid_row['Rule']
        rule_output = rule_cell_analysis_sgk(rule, sgk_cntr)
        sgk_cntr += 1
        if rule_output != " ":
            left_joins += rule_output + "\n"
    return left_joins

def rule_cell_analysis_sgk(i_rule_cell_value, sgk_cntr):
    print("rule_cell_value:\n", i_rule_cell_value)
    source_key = " "
    edw_key = " "
    SGK_left_join_clause = " "
    sgk_tbl = " "
    sgk_alias = 'SGK{}'.format(str(sgk_cntr))
    stg_alias = 'STG'
    sgk_id_value = " "
    rule_cell_value = str(i_rule_cell_value).upper()
    if "LOOKUP" in rule_cell_value:
        strt_index = rule_cell_value.find("LOOKUP")+len("LOOKUP")
        rule_sbstring = rule_cell_value[strt_index:]
        rule_sbstring_list = rule_sbstring.split()
        # print("rule_sbstring_list", rule_sbstring_list)
        sgk_cntr = 0
        # print("len(rule_cell_value)", len(rule_sbstring_list), "__", len(rule_sbstring_list)-1 )
        for i in range(len(rule_sbstring_list)-1):
           # print("**", i, "..........\n")
           if i == 0:
               source_key = rule_sbstring_list[i]
               # print("--source_key:", source_key)

           elif "AGAINST" in rule_sbstring_list[i].upper():
               edw_key = rule_sbstring_list[i + 1]
               # print("--edw_key:", edw_key)

           elif "SGK" in rule_sbstring_list[i].upper():
               if sgk_cntr == 0:
                   sgk_tbl = rule_sbstring_list[i]
                   # print("--sgk_tbl:", sgk_tbl)
                   sgk_cntr += 1
                   # left_join_clause = "LEFT JOIN {}.{}{}\n ON \nAND ".format("dd_fsdm", sgk_tbl, sgk_alias)
               else:
                   sgk_id_value = rule_sbstring_list[i+2]
                   # print("--sgk_id_value:", sgk_id_value)
                   # and_clause = "SGK.{} = {}".format(rule_sbstring_list[i], rule_sbstring_list[i+2])

        SGK_left_join_clause = "\nLEFT JOIN {}.{} {}\nON {}.{} = {}.{}\n    AND {}.SGK_ID = {}"\
                               .format("dd_fsdm", sgk_tbl, sgk_alias, sgk_alias, edw_key, stg_alias, source_key,
                                       sgk_alias, sgk_id_value)
        # print("SGK_left_join_clause:\n", SGK_left_join_clause)
    return SGK_left_join_clause


def get_current_date():
    return date.today().strftime("%Y-%m-%d")


def get_history_variables(smx_sheet, rid, table_name):
    smx_TFN_Rid = smx_sheet[smx_sheet['Record_ID'] == rid]
    smx_TFN_Rid = get_TFN_rid_no_tech_cols(smx_TFN_Rid) #remove the rows that have the tech cols from the df

    possible_start_date = []
    possible_end_date = []

    fsdm_tbl_col_list = smx_TFN_Rid['Column'].str.upper()
    fsdm_tbl_col_list = fsdm_tbl_col_list.tolist()

    for i in range(len(fsdm_tbl_col_list)):
        col_name = fsdm_tbl_col_list[i]
        if "_STRT_" in col_name:
            possible_start_date.append(col_name)
        elif "_END_" in col_name:
            possible_end_date.append(col_name)

    historization_keys = smx_TFN_Rid[smx_TFN_Rid['PK'].str.upper() == 'PK']['Column'].tolist()
    historization_columns = smx_TFN_Rid[smx_TFN_Rid['PK'].str.upper() != 'PK']['Column'].tolist()
    historization_keys = [item for item in historization_keys if item not in possible_start_date]
    historization_columns = [item for item in historization_columns if item not in possible_end_date]

    # print("possible_start_date: ", possible_start_date, "possible_end_date: ", possible_end_date, "historization_keys: ", historization_keys, "historization_columns: ", historization_columns )
    return possible_start_date, possible_end_date, historization_keys, historization_columns


def get_fsdm_tbl_columns(smx_Rid, alias_name):
    # smx_Rid = SMX_SHEET.loc[
    #     (SMX_SHEET['Record_ID'] == R_id)
    # ].reset_index()
    smx_Rid = smx_Rid.reset_index()
    columns_list = smx_Rid['Column'].values.tolist()
    columns_comma = ""
    if alias_name is None:
        alias = ''
    else:
        alias = alias_name + '.'
    for column_name in columns_list:
        comma = '    ' + ',' if columns_list.index(column_name) > 0 else '    '
        columns_comma += comma+alias+column_name + '\n'
    columns_comma = columns_comma[0:len(columns_comma) - 1]
    return columns_comma.strip()


def get_fsdm_tech_cols_list():
    tech_cols_list = ['R_ID', 'B_ID', 'INSRT_DTTM', 'UPDT_DTTM']
    return tech_cols_list

def get_fsdm_tbl_non_technical_columns(smx_Rid, alias_name):
    tech_cols_list = get_fsdm_tech_cols_list()

    smx_Rid = smx_Rid.reset_index()
    columns_list = smx_Rid['Column'].values.tolist()
    columns_list = np.setdiff1d(columns_list, tech_cols_list).tolist()
    columns_comma = ""
    if alias_name is None:
        alias = ''
    else:
        alias = alias_name + '.'
    for column_name in columns_list:
        comma = '    ' + ',' if columns_list.index(column_name) > 0 else '    '
        columns_comma += comma+alias+column_name + '\n'
    columns_comma = columns_comma[0:len(columns_comma) - 1]
    return columns_comma.strip()


def get_Rid_Source_System(SMX_Rid):
    src_system_names = SMX_Rid.loc[
        (SMX_Rid['Source_System'] != 'ETL')
    ]
    src_system_name = src_system_names['Source_System'].unique()[0]
    return src_system_name


def get_sama_table_columns_comma_separated(tables_sheet, Table_name, alias=None, record_id=None):
    if record_id is None:
        tables_df = tables_sheet.loc[
            (tables_sheet['TABLE_NAME'].str.upper() == Table_name.upper())
        ].reset_index()
    else:
        tables_df = tables_sheet.loc[
            (tables_sheet['Entity'].str.upper() == Table_name.upper())
            & (tables_sheet['Record_ID'] == record_id)].reset_index()
    columns_comma = ""
    if alias is None:
        alias = ''
    else:
        alias = alias+'.'
    for stg_tbl_indx, stg_tbl_row in tables_df.iterrows():
        if record_id is not None:
            comma = '    ' + ',' if stg_tbl_indx > 0 else '    '
        else:
            comma = '    ' + ',' if stg_tbl_indx > 0 else ''
        if alias == 'sgk.':
            alias = ''
            comma = '' + ',' if stg_tbl_indx > 0 else '    '
        if record_id is None:
            columns_comma += comma+alias+stg_tbl_row['COLUMN_NAME'] +'\n'
        else:
            columns_comma += comma+alias+stg_tbl_row['Column'] +'\n'
    columns_comma = columns_comma[0:len(columns_comma) - 1]
    return columns_comma


def get_comparison_columns(tables_sheet, Table_name, apply_type, operational_symbol, alias1=None, alias2=None, record_id=None):
    conditional_statement = ''
    columns_comma = ""
    if apply_type.upper() == "HISTORY":
        tables_df = tables_sheet.loc[(tables_sheet['Entity'].str.upper() == Table_name.upper())
                                     & ((tables_sheet['PK'].str.upper() == 'PK')|(tables_sheet['Historization column'].str.upper() == 'E'))
                                     & (tables_sheet['Record_ID'] == record_id)
                                     ].reset_index()
    elif apply_type.upper() == "HISTORY_COL":
        hist_col_list = get_history_variables(tables_sheet, record_id, Table_name)[3]
        tables_sheet = tables_sheet[tables_sheet.Column.isin(hist_col_list)]

        tables_df = tables_sheet.loc[(tables_sheet['Entity'].str.upper() == Table_name.upper())
                                     & (tables_sheet['PK'].str.upper() != 'PK')
                                     & (tables_sheet['Record_ID'] == record_id)#.any(axis = 0)
                                     ].reset_index()
        tables_df = tables_df[tables_df.Column.isin(hist_col_list)]

    elif apply_type.upper() == "INSERT" or apply_type.upper() == "UPSERT":

        tables_df = tables_sheet.loc[(tables_sheet['Entity'].str.upper() == Table_name.upper())
                                     & (tables_sheet['PK'].str.upper() != 'PK')
                                     & (tables_sheet['Record_ID'] == record_id)
                                     ].reset_index()
        tech_cols = get_fsdm_tech_cols_list()
        tables_df = tables_df[~tables_df.Column.isin(tech_cols)]
    else:
        tables_df = tables_sheet.loc[(tables_sheet['Entity'].str.upper() == Table_name.upper())
                                     & (tables_sheet['PK'].str.upper() == 'PK')
                                     & (tables_sheet['Record_ID'] == record_id)
                                     ].reset_index()
        tech_cols = get_fsdm_tech_cols_list()
        tables_df = tables_df[~tables_df.Column.isin(tech_cols)]

    if alias1 is None:
        alias1 = ''
    else:
        alias1 = alias1 + '.'
    if alias2 is None:
        alias2 = ''
    else:
        alias2 = alias2 + '.'

    for stg_tbl_indx, stg_tbl_row in tables_df.iterrows():
        data_type = stg_tbl_row['Datatype'].upper()
        column_name = str(stg_tbl_row['Column'])
        numeric_data_types = ['INTEGER', 'BIGINT', 'SMALLINT', 'FLOAT']
        if data_type in numeric_data_types or 'DECIMAL' in data_type:
            column_name = 'COALESCE('+alias1 + column_name + ',-1) ' + operational_symbol + ' COALESCE('+alias2 + column_name + ',-1)'
        elif 'CHAR' in data_type:# == 'VARCHAR(50)':
            column_name = 'COALESCE('+alias1 + column_name + ",'-') " + operational_symbol + ' COALESCE('+alias2 + column_name + ",'-')"
        elif data_type == 'TIMESTAMP':
            column_name = 'COALESCE('+alias1 + column_name + ",CAST('1001-01-01 00:00:00' AS TIMESTAMP(0))) " + operational_symbol + ' COALESCE('+alias2 + column_name + ",CAST('1001-01-01 00:00:00' AS TIMESTAMP(0)))"
        elif data_type == 'DATE':
            column_name = 'COALESCE('+alias1 + column_name + ",CAST('1001-01-01' AS DATE)) " + operational_symbol + ' COALESCE('+alias2 + column_name + ",CAST('1001-01-01' AS DATE'))"

        comma = '    ' + '    AND ' if stg_tbl_indx > 0 else ' '
        columns_comma += comma+column_name+'\n'
    columns_comma = columns_comma[0:len(columns_comma) - 1]
    return columns_comma


def get_sama_pk_columns_comma_separated(tables_sheet, Table_name, alias='', record_id=None):
    columns_df = get_sama_stg_table_columns_pk(tables_sheet,Table_name,record_id)
    columns_comma = ""
    if alias is None:
        alias = ''
    else:
        alias = alias + '.'
    for stg_tbl_indx, stg_tbl_row in columns_df.iterrows():
        comma = '    ' + ',' if stg_tbl_indx > 0 else ''
        if alias == 'one_pk.':
            return stg_tbl_row['Column']
        if record_id is None:
            columns_comma += comma+alias+str(stg_tbl_row['COLUMN_NAME']) +'\n'
        else:
            columns_comma += comma+alias+str(stg_tbl_row['Column'])+'\n'
    columns_comma = columns_comma[0:len(columns_comma) - 1]
    return columns_comma


def get_sama_stg_table_columns_minus_pk(tables_sheet, Table_name,record_id=None):
    if record_id is None:
        tables_df = tables_sheet.loc[(tables_sheet['TABLE_NAME'].str.upper() == Table_name.upper())
                                     & (tables_sheet['PRIMARY_KEY_FLAG'].str.upper() != 'Y')
                                     ].reset_index()
    else:
        tables_df = tables_sheet.loc[(tables_sheet['Entity'].str.upper() == Table_name.upper())
                                     & (tables_sheet['PK'].str.upper() != 'PK')
                                     & (tables_sheet['Record_ID'] == record_id)
                                     ].reset_index()
    return tables_df


def get_sama_stg_table_columns_pk(tables_sheet, Table_name, record_id=None, history_flag=None):
    if record_id is None:
        tables_df = tables_sheet.loc[(tables_sheet['TABLE_NAME'].str.upper() == Table_name.upper())
                                         & (tables_sheet['PRIMARY_KEY_FLAG'].str.upper() == 'Y')
                                         ].reset_index()
    elif record_id is not None and history_flag is not None:
        tables_df = tables_sheet.loc[(tables_sheet['Entity'].str.upper() == Table_name.upper())
                                         & (tables_sheet['PK'].str.upper() == 'PK')
                                         & (tables_sheet['Record_ID'] == record_id)
                                         & (tables_sheet['Historization column'] != 'S')
                                         ].reset_index()
    else:
        tables_df = tables_sheet.loc[(tables_sheet['Entity'].str.upper() == Table_name.upper())
                                         & (tables_sheet['PK'].str.upper() == 'PK')
                                         & (tables_sheet['Record_ID'] == record_id)
                                         ].reset_index()
    return tables_df


def get_sgk_record(SGK_tables,TABLENAME,RECORDID,flag):
    null_cols = ''
    tables_df = SGK_tables.loc[(SGK_tables['Entity'].str.upper() == TABLENAME.upper())
                                         & (SGK_tables['Record_ID'] == RECORDID)
                                         ].reset_index()
    null_tables_df = tables_df.loc[(tables_df['Rule'] == 'NULL') | (tables_df['Rule'] == '')
                                         ].reset_index()

    for null_tables_index,null_tables_row in null_tables_df.iterrows():
        Column_name = null_tables_row['Column']
        null_statment = 'NULL AS ' + Column_name
        and_Column_name = '\n\t' + null_statment + ','
        null_cols = null_cols + and_Column_name
    if flag == 'null_cols':
        return null_cols

    for tables_df_index,tables_df_row in tables_df.iterrows():
        if tables_df_row['Rule'] != 'NULL' and tables_df_row['Rule'] != '':
            source_column = tables_df_row['Source_Column']
            sgk_key = tables_df_row['Column']
            src_key_dt = tables_df_row['Datatype']
            rule = tables_df_row['Rule']
            if flag == 'sgk_key':
                return sgk_key
            if flag == 'src_col':
                return source_column
            if flag == 'rule':
                return rule
            if flag == 'src_key' and rule == '1:1':
                return sgk_key
            if flag == 'data_type' and rule == '1:1':
                return src_key_dt


def get_aliased_columns(columns_list, alias=None):
    columns_comma = ""
    if alias is None:
        alias = ''
    else:
        alias = alias + '.'

    for i in range(len(columns_list)):
        comma = '    ' + ',' if i > 0 else ''
        col_name = columns_list[i]
        columns_comma += comma + alias + str(col_name) + '\n'
    columns_comma = columns_comma[0:len(columns_comma) - 1]
    return columns_comma


def get_hist_end_dt_updt(column_name, columns_type, operational_symbol, alias1=None, alias2=None, record_id=None):
    if alias1 is None:
        alias1 = ''
    else:
        alias1 = alias1 + '.'
    if alias2 is None:
        alias2 = ''
    else:
        alias2 = alias2 + '.'

    end_dt_updt = ""

    if columns_type == 'end_date' and record_id is not None:
        interval = " - INTERVAL '0.000001' SECOND"
        end_dt_updt = alias1 + column_name + ' ' + operational_symbol + ' ' + alias2 + column_name + interval
    return end_dt_updt


def get_conditional_stamenet(tables_sheet, Table_name,columns_type,operational_symbol,alias1=None,alias2=None,record_id=None,history_flag=None):
    conditional_statement = ''
    if columns_type == 'pk' and record_id is None:
        table_columns = get_sama_stg_table_columns_pk(tables_sheet,Table_name)
    elif columns_type != 'pk' and record_id is None:
        table_columns = get_sama_stg_table_columns_minus_pk(tables_sheet,Table_name)
    elif columns_type == 'pk' and record_id is not None:
        table_columns = get_sama_stg_table_columns_pk(tables_sheet,Table_name,record_id,history_flag)
    elif columns_type == 'non_pk' and record_id is not None:
        table_columns = get_sama_stg_table_columns_minus_pk(tables_sheet,Table_name,record_id)
    elif columns_type == 'non_pk_upsert_set' and record_id is not None:
        excluded_cols = ['R_ID', 'INSRT_DTTM']
        table_columns = get_sama_stg_table_columns_minus_pk(tables_sheet, Table_name, record_id)
        table_columns = table_columns[~table_columns.Column.isin(excluded_cols)]
    elif columns_type == 'hist_key_end_date' and record_id is not None:
        hist_keys_list = get_history_variables(tables_sheet, record_id, Table_name)[2]
        end_date_list = get_history_variables(tables_sheet, record_id, Table_name)[1]
        hist_keys_list.extend(end_date_list)

        table_columns = tables_sheet.loc[(tables_sheet['Entity'].str.upper() == Table_name.upper())
                                         & (tables_sheet['Record_ID'] == record_id)
                                         ].reset_index()
        table_columns = table_columns[table_columns.Column.isin(hist_keys_list)]

    elif columns_type == 'hist_key_strt_date' and record_id is not None:
        hist_keys_list = get_history_variables(tables_sheet, record_id, Table_name)[2]
        start_date_list = get_history_variables(tables_sheet, record_id, Table_name)[0]
        hist_keys_list.extend(start_date_list)
        table_columns = tables_sheet.loc[(tables_sheet['Entity'].str.upper() == Table_name.upper())
                                         & (tables_sheet['Record_ID'] == record_id)
                                         ].reset_index()
        table_columns = table_columns[table_columns.Column.isin(hist_keys_list)]

    if alias1 is None:
        alias1 = ''
    else:
        alias1 = alias1 + '.'
    if alias2 is None:
        alias2 = ''
    else:
        alias2 = alias2 + '.'
    for column_name_index, column_name_row in table_columns.iterrows():
        if record_id is None:
            Column_name = column_name_row['COLUMN_NAME']
        else:
            Column_name = column_name_row['Column']
        on_statement = alias1 + Column_name + ' ' + operational_symbol + ' ' + alias2 + Column_name
        if record_id is not None:
            and_statement = '        ' + 'and ' if column_name_index > 0 else ' '
        else:
            and_statement = '        ' + 'and ' if column_name_index > 0 else '    '

        on_statement = on_statement if column_name_index == len(table_columns) else on_statement + '\n'#len(table_columns) - 1 else on_statement + '\n'
        and_Column_name = and_statement + on_statement
        conditional_statement = conditional_statement + and_Column_name
    return conditional_statement.rstrip()


def single_quotes(string):
    return "'%s'" % string

def double_quotes(string):
    return '"%s"' % string



def assertions(table_maping_row, Core_tables_list):
    assert (table_maping_row['Main source'] != None), 'Missing Main Source  for Table Mapping:{}'.format(
        str(table_maping_row['Mapping name']))
    assert (table_maping_row[
                'Target table name'] in Core_tables_list), 'TARGET TABLE NAME not found in Core Tables Sheet for Table Mapping:{}'.format(
        str(table_maping_row['Mapping name']))


def list_to_string(list, separator=None, between_single_quotes=0):
    if separator is None:
        prefix = ""
    else:
        prefix = separator
    to_string = prefix.join(
        (single_quotes(str(x)) if between_single_quotes == 1 else str(x)) if x is not None else "" for x in list)

    return to_string


def string_to_dict(sting_dict, separator=' '):
    if sting_dict:
        # ex: Firstname="Sita" Lastname="Sharma" Age=22 Phone=1234567890
        return eval("dict(%s)" % ','.join(sting_dict.split(separator)))


def wait_for_processes_to_finish(processes_numbers, processes_run_status, processes_names):
    count_finished_processes = 0
    no_of_subprocess = len(processes_numbers)

    while processes_numbers:
        for p_no in range(no_of_subprocess):
            if processes_run_status[p_no].poll() is not None:
                try:
                    processes_numbers.remove(p_no)
                    count_finished_processes += 1
                    # print('-----------------------------------------------------------')
                    # print('\nProcess no.', p_no, 'finished, total finished', count_finished_processes, 'out of', no_of_subprocess)
                    print(count_finished_processes, 'out of', no_of_subprocess, 'finished.\t', processes_names[p_no])
                except:
                    pass


def xstr(s):
    if s is None:
        return ''
    return str(s)


def save_to_parquet(pq_df, dataset_root_path, partition_cols=None, string_columns=None):
    if not pq_df.empty:

        # all_object_columns = df.select_dtypes(include='object').columns
        # print(all_object_columns)

        if string_columns is None:
            # string_columns = df.columns
            string_columns = pq_df.select_dtypes(include='object').columns

        for i in string_columns:
            pq_df[i] = pq_df[i].apply(xstr)

        partial_results_table = pa.Table.from_pandas(df=pq_df, nthreads=None)

        pq.write_to_dataset(partial_results_table, root_path=dataset_root_path, partition_cols=partition_cols,
                            use_dictionary=False
                            )
        # flavor = 'spark'
        # print("{:,}".format(len(df.index)), 'records inserted into', dataset_root_path, 'in', datetime.datetime.now() - start_time)


def read_all_from_parquet(dataset, columns, use_threads, filter=None):
    try:
        df = pq.read_table(dataset,
                           columns=columns,
                           use_threads=use_threads,
                           use_pandas_metadata=True).to_pandas()

        if filter:
            df = df_filter(df, filter, False)
    except:
        df = pd.DataFrame()

    return df


def read_all_from_parquet_delayed(dataset, columns=None, filter=None):
    df = dd.read_parquet(path=dataset, columns=columns, engine='pyarrow')
    if filter:
        for i in filter:
            df = df[df[i[0]].isin(i[1])]
    return df


def get_sheet_path(parquet_db_name, smx_file_path, output_path, sheet_name):
    file_name = get_file_name(smx_file_path)
    parquet_path = output_path + "/" + file_name + "/" + parquet_db_name + "/" + sheet_name
    return parquet_path


def save_sheet_data(parquet_db_name, df, smx_file_path, output_path, sheet_name):
    parquet_path = get_sheet_path(parquet_db_name, smx_file_path, output_path, sheet_name)
    save_to_parquet(df, parquet_path, partition_cols=None, string_columns=None)


def get_sheet_data(parquet_db_name, smx_file_path, output_path, sheet_name, df_filter=None):
    parquet_path = get_sheet_path(parquet_db_name, smx_file_path, output_path, sheet_name)
    df_sheet = read_all_from_parquet(parquet_path, None, True, filter=df_filter)
    if not isinstance(df_sheet, pd.DataFrame):
        df_sheet = pd.DataFrame()
    return df_sheet


def is_smx_file(file, sheets):
    file_sheets = pd.ExcelFile(file).sheet_names
    required_sheets = list(sheets)
    for required_sheet in sheets:
        for file_sheet in file_sheets:
            if file_sheet == required_sheet:
                required_sheets.remove(required_sheet)

    return True if len(required_sheets) == 0 else False


def get_smx_files(smx_path, smx_ext, stg_sheets,smx_sheets,sheet_type):
    smx_files = []
    all_files = md.get_files_in_dir(smx_path, smx_ext)
    for i in all_files:
        file = smx_path + "/" + i
        if sheet_type == 'Staging Tables':
            smx_files.append(i) if is_smx_file(file, stg_sheets) else None
        elif sheet_type == 'SMX':
            smx_files.append(i) if is_smx_file(file, smx_sheets) else None
    return smx_files

def get_config_file_path():
    config_file_path = md.get_dirs()[1]
    return config_file_path


def get_config_file_values(config_file_path=None):
    separator = "$$$"
    parameters = ""
    # config_file_path = os.path.dirname(sys.modules['__main__'].__file__)
    if config_file_path is None:
        try:
            config_file_path = get_config_file_path()
            print("ana hna ", config_file_path)
            config_file = open(config_file_path + "/" + pm.default_config_file_name, "r")

        except:
            config_file_path = input("Enter config.txt path please:")
            config_file = open(config_file_path + "/" + pm.default_config_file_name, "r")
    else:
        try:
            config_file = open(config_file_path, "r")
        except:
            config_file = None

    if config_file:
        for i in config_file.readlines():
            line = i.strip()
            if line != "":
                if line[0] != '#':
                    parameters = parameters + line + separator

        param_dic = string_to_dict(parameters, separator)
        dt_now = dt.datetime.now()
        dt_folder = dt_now.strftime("%Y") + "_" + \
                    dt_now.strftime("%b").upper() + "_" + \
                    dt_now.strftime("%d") + "_" + \
                    dt_now.strftime("%H") + "_" + \
                    dt_now.strftime("%M") + "_" + \
                    dt_now.strftime("%S")
        param_dic['output_path'] = param_dic["home_output_folder"] + "/" + dt_folder
        param_dic['read_sheets_parallel'] = 1
    else:
        param_dic = {}
    return param_dic


def server_info():
    cpu_per = psutil.cpu_percent(interval=0.5, percpu=False)
    # cpu_ghz = psutil.cpu_freq()
    # io = psutil.disk_io_counters()
    mem_per = psutil.virtual_memory()[2]

    return (cpu_per, mem_per)


def get_model_col(df, table_name):
    core_tables_IDS = df[df['Column name'].str.endswith(str('_ID'))]
    for core_tables_index, core_tables_row in core_tables_IDS.iterrows():
        if core_tables_row['Table name'] == table_name:
            return core_tables_row['Column name']


class WriteFile:
    def __init__(self, file_path, file_name, ext, f_mode="w+", new_line=False):
        self.new_line = new_line
        self.f = open(os.path.join(file_path, file_name + "." + ext), f_mode, encoding="utf-8")

    def write(self, txt, new_line=None):
        self.f.write(txt)
        new_line = self.new_line if new_line is None else None
        self.f.write("\n") if new_line else None

    def close(self):
        self.f.close()


class SMXFilesLogError(WriteFile):
    def __init__(self, log_error_path, smx_file_name, system_row, error):
        self.log_error_path = log_error_path
        self.log_file_name = "log"
        self.ext = "txt"
        super().__init__(self.log_error_path, self.log_file_name, self.ext, "a+", True)
        self.smx_file_name = smx_file_name
        self.system_row = system_row
        self.error = error

    def log_error(self):
        error_separator = "##############################################################################"
        self.write(str(dt.datetime.now()))
        self.write(self.smx_file_name) if self.smx_file_name else None
        self.write(self.system_row) if self.system_row else None
        self.write(self.error)
        self.write(error_separator)


class TemplateLogError(WriteFile):
    def __init__(self, log_error_path, file_name_path, error_file_name, error):
        self.log_error_path = log_error_path
        self.log_file_name = "log"
        self.ext = "txt"
        super().__init__(self.log_error_path, self.log_file_name, self.ext, "a+", True)
        self.file_name_path = file_name_path
        self.error_file_name = error_file_name
        self.error = error

    def log_error(self):
        error_separator = "##############################################################################"
        self.write(str(dt.datetime.now()))
        self.write(self.file_name_path)
        self.write(self.error_file_name)
        self.write(self.error)
        self.write(error_separator)


if __name__ == "__main__":
   get_config_file_values()