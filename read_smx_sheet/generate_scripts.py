import os, sys, subprocess
sys.path.append(os.getcwd())
from read_smx_sheet.app_Lib import manage_directories as md, functions as funcs
from dask import compute, delayed, config
from dask.diagnostics import ProgressBar
from read_smx_sheet.templates import Staging,Data_Mart,BTEQ_Scripts
from read_smx_sheet.templates import Staging_template , BTEQ_template
from read_smx_sheet.parameters import parameters as pm
import traceback
import datetime as dt


class LogFile(funcs.WriteFile):
    def __init__(self, output_path):
        self.output_path = output_path
        self.file_name = "log"
        self.ext = "txt"
        super().__init__(self.output_path, self.file_name, self.ext, "a+", True)


class ConfigFile:
    def __init__(self, config_file=None, config_file_values=None):
        self.config_file_values = funcs.get_config_file_values(config_file) if config_file_values is None else config_file_values
        self.output_path = self.config_file_values["output_path"]
        self.smx_path = self.config_file_values["smx_path"]
        self.oi_prefix = self.config_file_values["oi_prefix"]
        self.stg_prefix = self.config_file_values["stg_prefix"]
        self.dm_prefix = self.config_file_values["dm_prefix"]
        self.bteq_run_file = self.config_file_values["bteq_run_file"]
        self.duplicate_table_suffix = self.config_file_values["duplicate_table_suffix"]
        self.read_sheets_parallel = self.config_file_values["read_sheets_parallel"]


class GenerateScripts:
    def __init__(self, config_file=None, config_file_values=None):
        self.start_time = dt.datetime.now()
        self.cf = ConfigFile(config_file, config_file_values)
        md.remove_folder(self.cf.output_path)
        md.create_folder(self.cf.output_path)
        self.log_file = LogFile(self.cf.output_path)
        self.error_message = ""
        self.parallel_remove_output_home_path = []
        self.parallel_create_output_home_path = []
        self.parallel_create_smx_copy_path = []
        self.parallel_used_smx_copy = []
        self.parallel_create_output_source_path = []
        self.parallel_templates = []
        self.count_sources = 0
        self.count_smx = 0
        self.smx_ext = pm.smx_ext
        self.STG_tables_sht = pm.STG_tables_sht
        self.SAMA_sheets = pm.SAMA_sheets
        self.Data_types_sht = pm.Data_types_sht

    def generate_scripts(self):
        self.log_file.write("Reading from: \t" + self.cf.smx_path)
        self.log_file.write("Output folder: \t" + self.cf.output_path)
        self.log_file.write("SMX files:")
        print("Reading from: \t" + self.cf.smx_path)
        print("Output folder: \t" + self.cf.output_path)
        print("SMX files:")
        filtered_sources = []
        self.start_time = dt.datetime.now()

        try:
            smx_files = funcs.get_smx_files(self.cf.smx_path, self.smx_ext, self.SAMA_sheets)
            for smx in smx_files:
                try:
                    self.count_smx = self.count_smx + 1
                    self.count_sources = 1
                    smx_file_path = self.cf.smx_path + "/" + smx
                    smx_file_name = os.path.splitext(smx)[0]
                    print("\t" + smx_file_name)
                    self.log_file.write("\t" + smx_file_name)
                    home_output_path = self.cf.output_path + "/" + smx_file_name + "/"
                    self.parallel_create_output_home_path.append(delayed(md.create_folder)(home_output_path))
                    main_output_path = home_output_path + "/" + "DDLs"
                    bteq_output_path = home_output_path + "/" + "BTEQ Scripts"
                    self.parallel_create_output_source_path.append(delayed(md.create_folder)(main_output_path))
                    self.parallel_create_output_source_path.append(delayed(md.create_folder)(bteq_output_path))
                    Data_Types = delayed(funcs.read_excel)(smx_file_path, sheet_name=self.Data_types_sht)
                    STG_tables = delayed(funcs.read_excel)(smx_file_path, sheet_name=self.STG_tables_sht)
                    self.parallel_templates.append(delayed(Staging.stg_tables_DDL)(self.cf, main_output_path, STG_tables, Data_Types))
                    self.parallel_templates.append(delayed(Data_Mart.data_mart_DDL)(self.cf, main_output_path, STG_tables, Data_Types))
                    self.parallel_templates.append(delayed(BTEQ_Scripts.bteq_script)(self.cf, bteq_output_path, STG_tables))
                    #self.parallel_templates.append(delayed(Staging_template.stg_temp_DDL)(self.cf, main_output_path,STG_tables,Data_Types,'Staging'))
                    #self.parallel_templates.append(delayed(Staging_template.stg_temp_DDL)(self.cf, main_output_path,STG_tables,Data_Types,'Data_mart'))
                    #self.parallel_templates.append(delayed(BTEQ_template.bteq_temp_script)(self.cf, bteq_output_path,STG_tables))


                except Exception as e_smx_file:
                    # print(error)
                    funcs.SMXFilesLogError(self.cf.output_path, smx, None, traceback.format_exc()).log_error()
                    self.count_smx = self.count_smx - 1
        except Exception as e1:
            self.elapsed_time = dt.datetime.now() - self.start_time
            funcs.SMXFilesLogError(self.cf.output_path, None, None, traceback.format_exc()).log_error()

        if len(self.parallel_templates) > 0:
            sources = funcs.list_to_string(filtered_sources, ', ')
            print("Sources:", sources)
            self.log_file.write("Sources:" + sources)
            scheduler_value = 'processes' if self.cf.read_sheets_parallel == 1 else ''
            with config.set(scheduler=scheduler_value):
                compute(*self.parallel_create_output_home_path)
                compute(*self.parallel_create_output_source_path)
            self.error_message = ""
        else:
            self.error_message = "No SMX Files Found!"

        with ProgressBar():
            smx_files = " smx files" if self.count_smx > 1 else " smx file"
            smx_file_sources = " sources" if self.count_sources > 1 else " source"
            print("Start generating " + str(len(self.parallel_templates)) + " script for " + str(
                self.count_sources) + smx_file_sources + " from " + str(self.count_smx) + smx_files)
            compute(*self.parallel_templates)
            self.log_file.write(str(len(self.parallel_templates)) + " script generated for " + str(
                self.count_sources) + smx_file_sources + " from " + str(self.count_smx) + smx_files)
            self.elapsed_time = dt.datetime.now() - self.start_time
            self.log_file.write("Elapsed Time: " + str(self.elapsed_time))

        if sys.platform == "win32":
            os.startfile(self.cf.output_path)
        else:
            opener = "open" if sys.platform == "darwin" else "xdg-open"
            subprocess.call([opener, self.cf.output_path])

        self.log_file.close()



