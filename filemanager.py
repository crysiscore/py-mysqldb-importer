import tarfile
import logging
import os
import shutil
import sys


def matchfacilityname(arr_facilities,f_name):
    facility = ''
    for f in arr_facilities:
        # Alguns nomes nao serao encontrados na lista (config.yaml)
        # casos em que se escreve mal o nome da US no script de backup
        # nestes casos deve-se corrigir o nome no script de backup (ex: Malhagalene -> Malhangalene)
        if f.lower() in f_name.lower():
            facility = f
            break
        else:
            facility = f_name.lower()[:len(f_name) - 4]  # No match found, remove .sql extension
    return facility


def decompressfile(filename, outdir, destdir):
    if tarfile.is_tarfile(filename):
        try:
            t = tarfile.open(filename, 'r')
            for member in t.getmembers():
                if os.path.splitext(member.name)[1] == ".sql":
                    t.extractall(path=outdir)
                    finalise_extraction(outdir, destdir)

        except tarfile.ReadError:
            logging.debug("File: " + member.name + " is somehow  invalid/ file is opened")
            print("File: " + member.name + " is somehow  invalid/ file is opened")
        except tarfile.CompressionError:
            logging.debug("File: " + member.name + " cannot be decoded properly")
            print("File: " + member.name + " cannot be decoded properly")
        except tarfile.TarError:
            logging.debug("File: " + member.name + "  cant be extracted")
            print("File: " + member.name + "  cant be extracted " + Tar)
        except:
            logging.debug("File: " + member.name + "Unexpected error:", sys.exc_info()[0])
            print("File: " + member.name + " Unexpected error: ", sys.exc_info()[0])
    else:
        logging.debug(filename + " is not an .tar file")
        print(filename + " is not an .tar file")

    return


def renamebackupfiles(vector_facilities,sql_files_dir):
    if os.path.isdir(sql_files_dir):
        os.chdir(sql_files_dir)
        for root, dirs, files in os.walk('.', topdown=True):
            for f in files:
                if f.endswith(".sql"):
                    new_name = matchfacilityname(vector_facilities,f)
                    os.rename(f, 'cs_' + new_name + '.sql')

    return


def sql_files(members):
    for tarinfo in members:
        if os.path.splitext(tarinfo.name)[1] == ".sql":
            yield tarinfo


def finalise_extraction(outdir, dest_dir):
    # create temp dir to put the .sql file
    # os.mkdir(outdir + "/" + filename[:len(filename) - 4])
    for path, dirs, files in os.walk(outdir, topdown=True):
        for name in files:
            if name.endswith(".sql"):
                logging.info("moving file: " + name + " to sql_data_files dir")
                print("moving file: " + name + " to sql_data_files dir")
                # Move the file to sql_data_files dir
                try:
                    shutil.move(src=os.path.join(path, name), dst=dest_dir)
                    logging.info("Removing temp directories")
                   # shutil.rmtree(path=outdir)
                except shutil.Error as why:
                    print(str(why))
                    logging.debug(str(why))
    return
