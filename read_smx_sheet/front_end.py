import os
import sys
sys.path.append(os.getcwd())
from read_smx_sheet.app_Lib import manage_directories as md, functions as funcs
import multiprocessing
from tkinter import *
from tkinter import messagebox, filedialog, ttk
from PIL import ImageTk, Image
from read_smx_sheet.parameters import parameters as pm
import read_smx_sheet.generate_scripts as gs
import datetime as dt
import traceback
import threading
import random


class FrontEnd:
    def __init__(self):
        self.root = Tk()
        img_icon = PhotoImage(file=os.path.join(md.get_dirs()[0], 'script_icon.png'))
        self.root.tk.call('wm', 'iconphoto', self.root._w, img_icon)
        self.root.wm_title("SMX Scripts Builder " + pm.ver_no)
        self.root.resizable(width="false", height="false")
        self.msg_no_config_file = "No Config File Found!"
        self.color_msg_no_config_file = "red"
        self.msg_ready = "Ready"
        self.color_msg_ready = "green"
        self.msg_generating = "In Progress... "
        self.color_msg_generating = "blue"
        self.msg_done = "Done, Elapsed Time: "
        self.color_msg_done = "green"
        self.color_msg_done_with_error = "red"
        self.color_error_messager = "red"

        frame_row0 = Frame(self.root, borderwidth="2", relief="ridge",width=72)
        frame_row0.grid(column=0, row=0, sticky=W)

        frame_row1 = Frame(self.root, borderwidth="2", relief="ridge",width=30)
        frame_row1.grid(column=0, row=1, sticky=W)

        frame_row2 = Frame(self.root, borderwidth="1", relief="ridge")
        frame_row2.grid(column=0, row=2, sticky=W + E)

        frame_processing_values = Frame(frame_row2, borderwidth="1", relief="ridge")
        frame_processing_values.grid(column=0, row=0, sticky="w")

        frame_row2_l = Frame(frame_processing_values, borderwidth="2", relief="ridge")
        frame_row2_l.grid(column=0, row=3, sticky=W+E+N+S)

        frame_row2_r = Frame(frame_processing_values, borderwidth="2", relief="ridge")
        frame_row2_r.grid(column=1, row=3, sticky=W+E+N+S)

        frame_row2_rr = Frame(frame_processing_values,relief="ridge")
        frame_row2_rr.grid(column=2, row=3, sticky=E+N)

        self.status_label_text = StringVar()
        self.status_label = Label(frame_row2_l,width=47,height=2)
        self.status_label.grid(column=0, row=0,sticky='e')

        self.server_info_label_text = StringVar()
        self.server_info_label = Label(frame_row2_r,width=51,height=2)
        self.server_info_label.grid(column=1, row=0,sticky='e')

        abs_file_path = os.path.join(md.get_dirs()[0], 'Teradata_logo-two_color.png')
        img = Image.open(abs_file_path, 'r')
        resized = img.resize((138,45), Image.ANTIALIAS)
        resized_image = ImageTk.PhotoImage(resized)
        self.image_label = Label(frame_row2_rr,image=resized_image,height=35)
        self.image_label.grid(column=2, row=0,  sticky=S+W+N+E)

        config_file_label = Label(frame_row0, text="Config File")
        config_file_label.grid(row=0, column=0, sticky='e')

        self.config_file_browse_button = Button(frame_row0, text="...", command=self.browsefunc)
        self.config_file_browse_button.grid(row=0, column=3, sticky='w')

        self.config_file_entry_txt = StringVar()
        self.config_file_entry = Entry(frame_row0, textvariable=self.config_file_entry_txt, width=104)
        config_file_path = os.path.join(funcs.get_config_file_path(), pm.default_config_file_name)
        try:
            x = open(config_file_path)
        except:
            config_file_path = ""
        self.config_file_entry.insert(END, config_file_path)
        self.config_file_entry.grid(row=0, column=1)

        frame_buttons = Frame(frame_row1, borderwidth="1", relief="ridge")
        frame_buttons.grid(column=1, row=0 , sticky="w")

        self.generate_button = Button(frame_buttons, text="Start", width=15,height=2, command=self.start)
        self.generate_button.grid(row=2, column=0)
        close_button = Button(frame_buttons, text="Exit", width=15,height=1, command=self.close)
        close_button.grid(row=3, column=0)

        frame_config_file_values = Frame(frame_row1, borderwidth="2", relief="ridge")
        frame_config_file_values.grid(column=0, row=0, sticky="w")

        frame_checkboxes_values = Frame(frame_config_file_values, relief="ridge")
        frame_checkboxes_values.grid(column=1, row=6, sticky="W")

        self.get_config_file_values()
        frame_config_file_values_entry_width = 88

        read_from_smx_label = Label(frame_config_file_values, text="SMXs Folder")
        read_from_smx_label.grid(row=0, column=0, sticky='e')

        self.text_field_read_from_smx = StringVar()
        self.entry_field_read_from_smx = Entry(frame_config_file_values, textvariable=self.text_field_read_from_smx, width=frame_config_file_values_entry_width)
        self.entry_field_read_from_smx.grid(row=0, column=1, sticky="w")

        output_path_label = Label(frame_config_file_values, text="Output Folder")
        output_path_label.grid(row=1, column=0, sticky='e')

        self.text_field_output_path = StringVar()
        self.entry_field_output_path = Entry(frame_config_file_values, textvariable=self.text_field_output_path, width=frame_config_file_values_entry_width)
        self.entry_field_output_path.grid(row=1, column=1, sticky="w")

        self.excel_sheet = StringVar()
        self.populate_config_file_values()

        self.config_file_entry_txt.trace("w", self.refresh_config_file_values)

        thread0 = GenerateScriptsThread(0, "Thread-0", self)
        thread0.start()

        self.root.mainloop()


    def change_status_label(self, msg, color):
        self.status_label_text.set(msg)
        self.status_label.config(fg=color, text=self.status_label_text.get())

    def change_server_info_label(self, msg, color):
        try:
            self.server_info_label_text.set(msg)
            self.server_info_label.config(fg=color, text=self.server_info_label_text.get())
        except RuntimeError:
            pass

    def get_config_file_values(self):
        try:
            self.config_file_values = funcs.get_config_file_values(self.config_file_entry_txt.get())
            self.smx_path = self.config_file_values["smx_path"]
            self.output_path = self.config_file_values["output_path"]
            self.oi_prefix = self.config_file_values["oi_prefix"]
            self.stg_prefix = self.config_file_values["stg_prefix"]
            self.dm_prefix = self.config_file_values["dm_prefix"]
            self.duplicate_table_suffix = self.config_file_values["duplicate_table_suffix"]
            self.bteq_run_file = self.config_file_values["bteq_run_file"]
            self.generate_button.config(state=NORMAL)
            self.change_status_label(self.msg_ready, self.color_msg_ready)
        except:
            self.change_status_label(self.msg_no_config_file, self.color_msg_no_config_file)
            self.generate_button.config(state=DISABLED)
            self.smx_path = ""
            self.output_path = ""

    def refresh_config_file_values(self, *args):
        self.get_config_file_values()
        self.populate_config_file_values()

    def populate_config_file_values(self):
        self.entry_field_read_from_smx.config(state=NORMAL)
        self.entry_field_read_from_smx.delete(0, END)
        self.entry_field_read_from_smx.insert(END, self.smx_path)
        self.entry_field_read_from_smx.config(state=DISABLED)

        self.entry_field_output_path.config(state=NORMAL)
        self.entry_field_output_path.delete(0, END)
        self.entry_field_output_path.insert(END, self.output_path)
        self.entry_field_output_path.config(state=DISABLED)


    def browsefunc(self):
        current_file = self.config_file_entry_txt.get()
        filename = filedialog.askopenfilename(initialdir=md.get_dirs()[1])
        filename = current_file if filename == "" else filename
        self.config_file_entry.delete(0, END)
        self.config_file_entry.insert(END, filename)
        self.refresh_config_file_values()

    def pb(self, tasks, task_len):
        self.progress_var = IntVar()
        pb = ttk.Progressbar(self.root, orient="horizontal",
                             length=300, maximum=task_len - 1,
                             mode="determinate",
                             var=self.progress_var)
        pb.grid(row=3, column=1)

        for i, task in enumerate(tasks):
            self.progress_var.set(i)
            i += 1
            # time.sleep(1 / 60)
            # compute(task)
            self.root.update_idletasks()

    def enable_disable_fields(self, f_state):
        self.generate_button.config(state=f_state)
        self.config_file_entry.config(state=f_state)
        self.config_file_browse_button.config(state=f_state)

    def generate_scripts_thread(self):
        try:
            config_file_path = self.config_file_entry_txt.get()
            x = open(config_file_path)
            try:
                self.enable_disable_fields(DISABLED)
                self.g.generate_scripts()
                self.enable_disable_fields(NORMAL)

                print("Total Elapsed time: ", self.g.elapsed_time, "\n")
            except Exception as error:
                try:
                    error_messager = self.g.error_message
                except:
                    error_messager = error
                self.change_status_label(error_messager, self.color_error_messager)
                self.generate_button.config(state=NORMAL)
                self.config_file_entry.config(state=NORMAL)
                traceback.print_exc()
        except:
            self.change_status_label(self.msg_no_config_file, self.color_msg_no_config_file)

    def destroyer(self):
        self.root.quit()
        self.root.destroy()
        sys.exit()

    def close(self):
        self.root.protocol("WM_DELETE_WINDOW", self.destroyer())

    def start(self):
        self.refresh_config_file_values()
        self.g = gs.GenerateScripts(None, self.config_file_values)

        thread1 = GenerateScriptsThread(1, "Thread-1", self)
        thread1.start()

        thread2 = GenerateScriptsThread(2, "Thread-2", self, thread1)
        thread2.start()

    def generating_indicator(self, thread):
        def r():
            return random.randint(0, 255)

        while thread.is_alive():
            elapsed_time = dt.datetime.now() - self.g.start_time
            msg = self.msg_generating + str(elapsed_time)
            # color_list = ["white", "black", "red", "green", "blue", "cyan", "yellow", "magenta"]
            # color = random.choice(color_list)
            color = '#%02X%02X%02X' % (r(),r(),r())
            self.change_status_label(msg, color)

        message = self.g.error_message if self.g.error_message != "" else self.msg_done + str(self.g.elapsed_time)
        color = self.color_msg_done_with_error if self.g.error_message != "" else self.color_msg_done
        self.change_status_label(message, color)

    def display_server_info(self, thread):
        color = "blue"
        while True:
            server_info = funcs.server_info()
            msg = "CPU " + str(server_info[0]) + "%" + " Memory " + str(server_info[1]) + "%"
            self.change_server_info_label(msg, color)


class GenerateScriptsThread(threading.Thread):
    def __init__(self,threadID ,name, front_end_c, thread=None):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name
        self.FrontEndC = front_end_c
        self.thread = thread
        self.daemon = True

    def run(self):
        if self.threadID == 1:
            self.FrontEndC.generate_scripts_thread()
        if self.threadID == 2:
            self.FrontEndC.generating_indicator(self.thread)
        if self.threadID == 0:
            self.FrontEndC.display_server_info(self.thread)


if __name__ == '__main__':
    multiprocessing.freeze_support()
    FrontEnd()
