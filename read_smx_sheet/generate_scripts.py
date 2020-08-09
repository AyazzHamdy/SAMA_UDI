import os, sys, subprocess
sys.path.append(os.getcwd())
from read_smx_sheet.app_Lib import manage_directories as md, functions as funcs
from dask import compute, delayed, config
from dask.diagnostics import ProgressBar
from read_smx_sheet.templates import Staging_DDL, BTEQ_Scripts, Apply_Insert_Upsert, History_Apply, TFN_insertion
from read_smx_sheet.templates import SGK_insertion
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
    def __init__(self,smx_type, config_file=None, config_file_values=None):
        self.config_file_values = funcs.get_config_file_values(config_file) if config_file_values is None else config_file_values
        self.output_path = self.config_file_values["output_path"]
        self.smx_path = self.config_file_values["smx_path"]
        try:
            self.templates_path = self.config_file_values["templates_path"]
        except:
            self.templates_path= self.config_file_values["smx_path"]
        self.bteq_run_file = self.config_file_values["bteq_run_file"]
        self.read_sheets_parallel = self.config_file_values["read_sheets_parallel"]
        self.stg_prefix = self.config_file_values["stg_prefix"]
        if smx_type == 'Staging Tables':
            self.oi_prefix = self.config_file_values["oi_prefix"]
            self.dm_prefix = self.config_file_values["dm_prefix"]
            self.duplicate_table_suffix = self.config_file_values["duplicate_table_suffix"]
        elif smx_type == 'SMX':
            self.ld_prefix = self.config_file_values["ld_prefix"]
            self.modelDB_prefix = self.config_file_values["fsdm_prefix"]
            self.modelDup_prefix = self.config_file_values["dup_prefix"]
            try:
                self.sgk_source = self.config_file_values["SGK_Source"]
            except:
                self.sgk_source = 'ALL'


class GenerateScripts:
    def __init__(self, config_file=None, config_file_values=None,project_generation_flag='Staging Tables'):
        self.start_time = dt.datetime.now()
        self.cf = ConfigFile(project_generation_flag,config_file, config_file_values)
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
        self.staging_sheets = pm.staging_sheets
        self.smx_sheets = pm.smx_sheets
        self.Data_types_sht = pm.Data_types_sht
        self.smx_sheet = pm.smx_sht
        self.scripts_generation_flag = ""

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
            smx_files = funcs.get_smx_files(self.cf.smx_path, self.smx_ext, self.staging_sheets,self.smx_sheets
                                            ,self.scripts_generation_flag)
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
                    if self.scripts_generation_flag=='Staging Tables':
                        main_output_path = home_output_path + "/" + "DDLs"
                        bteq_stg_dm_scripts_output_path = home_output_path + "/" + "BTEQ_Scrtipts" + "/" + "BTEQ_STG_TO_DATAMARAT_SCRIPTS"
                        bteq_stg_oi_scripts_output_path = home_output_path + "/" + "BTEQ_Scrtipts" + "/" + "BTEQ_STG_TO_OI_SCRIPTS"
                        self.parallel_create_output_source_path.append(delayed(md.create_folder)(main_output_path))
                        self.parallel_create_output_source_path.append(delayed(md.create_folder)(bteq_stg_dm_scripts_output_path))
                        self.parallel_create_output_source_path.append(delayed(md.create_folder)(bteq_stg_oi_scripts_output_path))
                        Data_Types = delayed(funcs.read_excel)(smx_file_path, sheet_name=self.Data_types_sht)
                        STG_tables = delayed(funcs.read_excel)(smx_file_path, sheet_name=self.STG_tables_sht)
                        self.parallel_templates.append(delayed(Staging_DDL.stg_temp_DDL)(self.cf, main_output_path, STG_tables, Data_Types, 'Staging'))
                        self.parallel_templates.append(delayed(Staging_DDL.stg_temp_DDL)(self.cf, main_output_path, STG_tables, Data_Types, 'Data_mart'))
                        self.parallel_templates.append(delayed(Staging_DDL.stg_temp_DDL)(self.cf, main_output_path, STG_tables, Data_Types, 'OI_staging'))
                        self.parallel_templates.append(delayed(BTEQ_Scripts.bteq_temp_script)(self.cf, bteq_stg_dm_scripts_output_path, STG_tables, 'from stg to datamart'))
                        self.parallel_templates.append(delayed(BTEQ_Scripts.bteq_temp_script)(self.cf, bteq_stg_oi_scripts_output_path, STG_tables, 'from stg to oi'))
                    elif self.scripts_generation_flag == 'SMX':
                        main_output_path_apply = home_output_path + "/" + "APPLY_SCRIPTS"
                        main_output_path_sgk = home_output_path + "/" + "SGK"
                        main_output_path_TFN = home_output_path + "/" + "TFN"
                        self.parallel_create_output_source_path.append(delayed(md.create_folder)(main_output_path_apply))
                        self.parallel_create_output_source_path.append(delayed(md.create_folder)(main_output_path_sgk))
                        self.parallel_create_output_source_path.append(delayed(md.create_folder)(main_output_path_TFN))
                        smx_sheet = delayed(funcs.read_excel)(smx_file_path, sheet_name=self.smx_sheet)
                        Rid_list = [30060, 30061, 30062, 30063, 30064, 30065, 30066
                            , 30067, 30069, 30070, 30071, 30072, 30073, 30074, 30075
                            , 30076, 30077, 30079, 30080, 30081, 30082, 30083, 30084
                            , 30085, 30086, 30087, 30089, 30090, 30091, 30092, 30093
                            , 30094, 30095, 30096, 30097, 30099, 30100, 30101, 30102
                            , 30103, 30104, 30105, 30106, 30107, 30109, 30110, 30111
                            , 30112, 30113, 30114, 30115, 30116, 30117, 30119, 30120
                            , 30121, 30122, 30123, 30124, 30125, 30126, 30127, 30129
                            , 30130, 30131, 30132, 30133, 30134, 30135, 30136
                            , 30137, 30139, 30140, 30141, 30142, 30143, 30144, 30145
                            , 30146, 30147, 30149, 30150, 30151, 30152, 30153, 30154
                            , 30155, 30156, 30157, 30159, 30160, 30161, 30162, 30163
                            , 30164, 30165, 30166, 30167, 30169, 30170, 30171, 30172
                            , 30173, 30174, 30175, 30176, 30177, 30179, 30180, 30181
                            , 30182, 30183, 30184, 30185, 30186, 30187, 30189, 30190
                            , 30191, 30192, 30193, 30194, 30195, 30196, 30197
                            , 30199, 30200, 30201, 30202, 30203, 30204, 30205, 30206
                            , 30207, 30209, 30210, 30211, 30212, 30213, 30214, 30215
                            , 30216, 30217, 30219, 30220, 30221, 30222, 30223, 30224
                            , 30225, 30226, 30227, 30229, 30391, 30392, 30393
                            , 30394, 30395, 30396, 30397, 30398, 30399, 30400, 30401
                            , 30402, 30403, 30404, 30405, 30406, 30407, 30408, 30409
                            , 30410, 30411, 30412, 30413, 30414, 30415
                            , 30416, 30417, 30418, 30419, 30420, 30421, 30422, 30423
                            , 30424, 30425, 30426, 30427, 30428, 30429, 30430, 30431
                            , 30432, 30433, 30434, 30435, 30436, 30437, 30438, 30439
                            , 30440, 30441, 30442, 30443, 30444, 30445, 30446, 30447
                            , 30448, 30449, 30450, 30451, 30452, 30453, 30454
                            , 30455, 30456, 30457, 30458]#zbk_
                        # Rid_list = [30390,30389,30379,30369,30373,30375,30377,30371,30381,30367,
                        #             30365,30355,30351,30387,30385,30353,30383,30361,
                        #             30359,30363,30357,30469,30471,30480,30342,30341]#Tahseen
                        # Rid_list = [30013, 30015, 30018, 30020,
                        #             30021, 30022, 30023, 30024,
                        #             30025, 30026, 30027, 30028,
                        #             30029, 30030, 30031, 30032,
                        #             30034, 30035, 30037, 30038,
                        #             30040, 30041, 30043, 30044,
                        #             30046, 30055, 30056, 30057,
                        #             30058, 30348, 30459, 30461,
                        #             30463, 30465, 30467, 30473,
                        #             30475, 30476, 30479] #mujahid
                        Rid_list =[30337,30048, 30059, 30333, 30050,30051,30049,30336] #my_8_trns
                        Rid_list =[30161,30062,30110,30162,30213,30180,30210,30102,
                                30083,30183,30080,30211,30113,30081,30130,30181,
                                30153,30150,30111,30133,30212,30173,30070,30170,
                                30220,30151,30082,30182,30131,30190,30073,
                                30171,30140,30112,30090,30123,30120,30071,
                                30223,30143,30200,30093,30193,30132,30160,30121,
                                30100,30221,30203,30141,30152,30063,30091,30163,
                                30060,30191,30072,30172,30103,30222,30201,30092,
                                30192,30061,30122,30142,30101,30202] #zbk__PRTY_CLAS_VAL
                        Rid_list = [30224,30067,30094,30194,30124,30144,30204,30085,
                                30185,30215,30115,30116,30186,30064,30216,30164,
                                30104,30187,30217,30086,30135,30136,30117,30087,
                                30155,30075,30175,30225,30137,30156,30095,30214,
                                30076,30125,30176,30157,30226,30205,30077,30177,
                                30227,30195,30084,30184,30196,30145,30146,30114,
                                30096,30065,30165,30126,30105,30097,30197,30206,
                                30127,30154,30147,30134,30207,30166,30174,30106,
                                30167,30107,30066,30074] #zbk__PRTY_PRTY_CLAS_XREF
                        Rid_list = [30038,30044,30390,30348,30027,30018,30045,30481] #for tahseen
                        smx_sheet = smx_sheet[smx_sheet.Record_ID.isin(Rid_list)]
                        self.parallel_templates.append(delayed(Apply_Insert_Upsert.apply_insert_upsert)(self.cf, main_output_path_apply, smx_sheet, "Apply_Insert"))
                        self.parallel_templates.append(delayed(Apply_Insert_Upsert.apply_insert_upsert)(self.cf, main_output_path_apply, smx_sheet, "Apply_Upsert"))
                        self.parallel_templates.append(delayed(History_Apply.history_apply)(self.cf, main_output_path_apply, smx_sheet))
                        self.parallel_templates.append(delayed(SGK_insertion.sgk_insertion)(self.cf, main_output_path_sgk, smx_sheet))
                        self.parallel_templates.append(delayed(TFN_insertion.TFN_insertion)(self.cf, main_output_path_TFN, smx_sheet))

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



