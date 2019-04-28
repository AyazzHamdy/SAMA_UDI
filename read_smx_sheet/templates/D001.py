from read_smx_sheet.parameters import parameters as pm
from read_smx_sheet.app_Lib import functions as funcs


def d001(source_output_path, source_name, STG_tables):
    file_name = funcs.get_file_name(__file__)
    f = open(source_output_path + "/" + file_name + ".sql", "w+")

    try:
        stg_tables_df = funcs.get_stg_tables(STG_tables, source_name=None)
        for STG_tables_index, STG_tables_row in stg_tables_df.iterrows():
            Table_name = STG_tables_row['Table name']

            f.write("delete from " + pm.GCFR_t + "." + pm.SOURCE_TABLES_LKP_table + " where SOURCE_NAME = '" + source_name + "' and TABLE_NAME = '" + Table_name + "';\n")
            f.write("insert into " + pm.GCFR_t + "." + pm.SOURCE_TABLES_LKP_table + "(SOURCE_NAME, TABLE_NAME)\n")
            f.write("VALUES ('" + source_name + "', '" + Table_name + "')" + ";\n")
            f.write("\n")
    except:
        pass

    f.close()