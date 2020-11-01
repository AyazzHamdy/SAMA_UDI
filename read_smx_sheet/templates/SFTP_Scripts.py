import pysftp
from read_smx_sheet.Logging_Decorator import Logging_decorator
cnopts = pysftp.CnOpts()
cnopts.hostkeys = None


@Logging_decorator
def sftp_script_generation(cf):
    print(cf.hostname)
    print(cf.username)
    print(cf.password)
    print(cf.source_path)
    print(cf.destination_path)
    print(cf.substring)

    with pysftp.Connection(host=cf.hostname, username=cf.username, password=cf.password, cnopts=cnopts) as sftp:
        sftp.cwd(cf.source_path)
        directory_structure = sftp.listdir_attr()

        for attr in directory_structure:
            if attr.filename.contains(cf.substring):
                file = attr.filename
                sftp.get(cf.source_path + file, cf.destination_path + file)
                print("Moved " + file + " to " + cf.destination_path)
