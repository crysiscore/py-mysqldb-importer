import os
import filemanager
import logging
import yaml
import dbacess

# Read config file
with open("/home/agnaldo/config.yaml", 'r') as stream:
    try:
        config = yaml.load(stream)

    except yaml.YAMLError as exc:
        logging.debug(str(exc))
    else:
        basedir = config['basedir']
        outdir = basedir + config['outdir']
        backup_dir = basedir + config['backup_dir']
        datadir = basedir + config['datadir']
        mysql_username = config['mysql_username']
        mysql_password = config['mysql_password']
if os.path.isdir(basedir):

    # log files
    logfile = "/pylog.log"
    logging.basicConfig(filename=outdir + logfile, format='%(asctime)s %(levelname)s:%(message)s',
                        level=logging.DEBUG)
    if os.path.isdir(backup_dir):
        logging.info("changing dir to : " + backup_dir)
        print("changing dir to : " + backup_dir)
        os.chdir(backup_dir)

        for path, dirs, files in os.walk('.', topdown=True):
            numtarfiles = 0
            for name in files:
                if name.endswith(".gz"):
                    # Untar all backupfiles
                    numtarfiles += 1
                    logging.info("File found: " + name)
                    print("File found: " + name)
                    #filemanager.decompressfile(name, outdir, datadir)
        if numtarfiles == 0:
            logging.info("No backup file was found:")
        else:
            print("Renaming sql_files in " + datadir)
            facilities = config['facilities']
            filemanager.renamebackupfiles(facilities,datadir)
            dbacess.beginmysqljob(numtarfiles, datadir)
        dbacess.beginmysqljob(numtarfiles, datadir)
    else:
        logging.debug("IOerror: path " + backup_dir + " was not found")
        print("IOerror: path " + backup_dir + " was not found")
else:
    logging.debug("IOerror: path " + basedir + " was not found")
    print("IOerror: path " + basedir + " was not found")

