#!/usr/bin/python
# -*- coding: utf-8 -*-

import argparse
import commands
import logging
import sys
from datetime import datetime, timedelta

# Number of archives to keep in the partitioned tables.
# Dev
score_flow_PartitionsToRetain = 2
weeklyPartitionsToRetain = 2
monthlyPartitionsToRetain = 3
# Prod
# score_flow_PartitionsToRetain=60
# weeklyPartitionsToRetain=12
# monthlyPartitionsToRetain=60


EDGE_NODE_LOCATION = sys.path[0]
SCORE_flow_FILE = 'score_flow_plus'


# UTILS
def bool_type(v):
    if v.lower() in ('yes', 'true', 't', 'y', '1'):
        return True
    elif v.lower() in ('no', 'false', 'f', 'n', '0'):
        return False
    else:
        raise argparse.ArgumentTypeError('Boolean value expected.')


def input_parameters():
    # Parameters
    parser = argparse.ArgumentParser(description="Flow to calculate and ingest score_flow(+) into Hive tables",
                                     usage='\n%(prog)s [<-archive=None>] [<-opsDB=ops_dev>] ['
                                           '<-stagingDB=staging_dev>] [<-prodDB=prod_dev>] [<-prodHDFS=prod_dev>] [ '
                                           '<-verbose=false>] [<-smail=False>] ['
                                           '<-notification_list=facundo.radrizzani@company.com>] [<-precedence=True>] '
                                           '[ '
                                           '<-first=False>] [<-env=dev>] [<-r_script=ctr_score_flow_plus.r>] [-h] ['
                                           '-v]\n\ni.e.: python %(prog)s -archive 20190101 '
                                           '-opsDB=ops_dev -verbose=true -env prod '
                                           '-notification_list=dest1@company.com dest2@company.com ')

    # Create mutually exclusive group: -first and archive can't be set together
    # Trying to set them at the same time: error: argument -archive: not allowed with argument -first
    group = parser.add_mutually_exclusive_group()
    group.add_argument('-archive', type=int, help='archive=YYYYMMDD. Default value will be last Saturday.', default=0)
    group.add_argument('-first', type=bool_type, help='Execute first of every month [True,False].', default=False)

    # If env = 'prod' then will use prod DBs and HDFS.
    parser.add_argument('-env',
                        help='Environment. If prod, then will set default parameters of DBs and HDFS to production.',
                        default='dev')
    parser.add_argument('-opsDB', help='[ops,ops_dev]', dest='ops_db', default='ops_dev')
    parser.add_argument('-stagingDB', help='[staging or staging_dev]', dest='staging_db', default='staging_dev')
    parser.add_argument('-prodDB', help='[ctr_comp_prod,prod_dev]', dest='prod_db', default='prod_dev')

    parser.add_argument('-prodHDFS', help='Production HDFS.[prod_dev,production]', default='prod_dev')

    parser.add_argument('-precedence', type=bool_type, help="Last time 'ctr_comp_indicadores' was ingested",
                        default=True)
    parser.add_argument('-smail', type=bool_type, help='Send notification mail?', default=False)
    parser.add_argument('-notification_list', help='Mailing list', default=['facundo.radrizzani@company.com'],
                        nargs='+')

    parser.add_argument('-r_script', help='R script to execute', default='ctr_score_flow_plus.r')

    parser.add_argument('-verbose', type=bool_type, help='Show more info in logs', dest='verbose', default=False)

    parser.add_argument('-v', action='version', version='%(prog)s version 5.0')

    args = parser.parse_args()

    # log_to_file(EDGE_NODE_LOCATION,params)
    # sys.exit("int para")

    global mail_list
    mail_list = args.notification_list

    global archive
    if args.archive == 0:  # If archive is empty, will put last Saturday OR YYYYMM01 if first using archive_func()
        archive = archive_func(args.first)
    else:
        archive = args.archive  # Chequear q el formato sea YYYYMMDD

    # Change original value to use in main program
    args.archive = archive

    global first
    first = args.first

    # Setting environment (production or dev). Default is dev options.
    global opsDef
    global stagingDef
    global prodDB
    global prodHDFS
    global env
    env = args.env
    if env == 'prod':
        opsDef = 'ops'
        stagingDef = 'staging'
        prodDB = 'ctr_comp_prod'
        prodHDFS = 'production'

        # To use it in main program
        args.ops_db = opsDef
        args.staging_db = stagingDef
        args.prod_db = prodDB
        args.prodHDFS = prodHDFS
    else:
        opsDef = args.ops_db
        stagingDef = args.staging_db
        prodDB = args.prod_db
        prodHDFS = args.prodHDFS

    global r_script
    r_script = args.r_script
    global verbose
    verbose = args.verbose
    global check_precedence
    check_precedence = args.precedence
    global smail
    smail = args.smail

    # global OPS_HDFS_FOLDER
    # OPS_HDFS_FOLDER ='/user/cluster_name/' + opsDef + '/comp/country/score_flow_plus'
    global PROD_HDFS_FOLDER
    # PROD_HDFS_FOLDER='/user/cluster_name/' + prodHDFS + '/comp/country/score_flow_plus'
    PROD_HDFS_FOLDER = '/user/cluster_name/' + prodHDFS + '/comp/country/score_flow'

    # return params
    return vars(args)


# severity_type='info' (default) - severity_type='error'
# More info: https://realpython.com/python-logging/
def log_to_file(local_path, msg, severity_type='info'):
    log_file = ("{path}/logs/output.log".format(path=local_path))

    '''
	level=logging.DEBUG
	level=logging.INFO *
	level=logging.WARNING
	level=logging.ERROR *
	level=logging.CRITICAL
	'''

    ff = "%(asctime)s - %(levelname)s - %(message)s"
    logging.basicConfig(filename=log_file, level=logging.INFO, filemode='a', format=ff, datefmt='%Y-%m-%d %H:%M:%S')

    if severity_type == 'info':
        logging.info(msg)
    elif severity_type == 'warning':
        logging.warning(msg)
    elif severity_type == 'error':
        logging.error(msg)
    else:
        logging.error("wrong severity_type for message '{msg}'".format(msg=msg))


# logging.debug('%s - DEGUG - %s' %(now,msg))
# logging.warning('%s - WARNING - %s' %(now,msg))
# logging.critical('%s - CRITICAL - %s' %(now,msg))


def send_mail(msg, local_path, archive):
    mail_message = msg

    if smail:
        # Need to do 'ssh -t' because mail is not working in edge_node node004 but it does in node001

        for mail in mail_list:
            # cmd = ("ssh -t node001 \"echo '{mail_message}' | mail -s 'ctr_SCORE_flow_PLUS - Archive={archive}' {
            # mail}\"" .format(mail_message=mail_message,archive=archive,mail=mail))

            cmd = ("echo '{mail_message}' | mail -s 'ctr_SCORE_flow_PLUS - Archive={archive}' {mail}".format(
                mail_message=mail_message, archive=archive, mail=mail))

            if verbose:
                msg2 = ("mailto:" + mail + " - cmd:" + cmd)
                log_to_file(local_path, msg2)
            else:
                msg2 = ("mailto:" + mail)
                log_to_file(local_path, msg2)

            try:
                status, output = commands.getstatusoutput(cmd)

                if status != 0:
                    raise RuntimeError("Can't send mail")
            except:
                log_to_file(local_path, "Sending email", 'error')
    return 0


def kerberos_auth(local_path, verbose, cancel_execution_if_errors=1):
    global output
    import getpass
    username = getpass.getuser()
    # print username

    if env == 'prod':
        keytab_file = '/export/home/dna-cb-apu-sys_prod/dna-cb-apu-sys_prod.keytab'
        cmd = f"kinit {username}@ERS.company.COM -k -t {keytab_file}"
    else:
        keytab_file = '/export/home/dna-cb-apu-sys_prod/dna-cb-apu-sys_prod.keytab'
        cmd = f"kinit {username}@ERS.company.COM -k -t {keytab_file}"

    if verbose:
        msg = f"kerberos_auth - Executing '{cmd}'"
        log_to_file(local_path, msg)

    try:
        status, output = commands.getstatusoutput(cmd)

        if status != 0:
            raise RuntimeError()

    except Exception as err:
        msg = f"kerberos_auth - {output} - {err}"
        log_to_file(local_path, msg, 'error')

        if cancel_execution_if_errors:
            msg = 'kerberos_auth - Cannot authenticate - End execution'
            log_to_file(local_path, msg, 'error')
            # send_mail(msg,local_path)
            sys.exit(-1)
    else:
        log_to_file(local_path, 'kerberos_auth - Auth Success')


# R SCORE flow
def execute_ctr_score_flow(local_path, db, r_script, archive, verbose, cancel_execution_if_errors=1):
    data_location = f"{local_path}/data"

    cmd = f"/usr/bin/Rscript {local_path}/r_script/{r_script} {archive} {data_location} {db} " \
          f"2>>{local_path}/logs/r_script.log >> {local_path}/logs/r_script.log "

    if verbose:
        msg = ("execute_ctr_score_flow_plus - Executing '{cmd}'".format(cmd=cmd))
        log_to_file(local_path, msg)
    else:
        msg = "execute_ctr_score_flow_plus - Executing"
        log_to_file(local_path, msg)

    try:
        status, output = commands.getstatusoutput(cmd)

        if status != 0:
            raise RuntimeError(f"{output} - error executing 'ctr_score_flow_plus.r' script")


    except:
        msg = ("execute_ctr_score_flow_plus - %s" % (sys.exc_info()[1]))
        log_to_file(local_path, msg, 'error')

        if cancel_execution_if_errors:
            msg = 'execute_ctr_score_flow_plus - Cannot execute_ctr_score_flow_plus - End execution'
            log_to_file(msg, 'error')
            # print(msg)
            # send_mail(local_path,msg)
            sys.exit(-1)
    else:
        log_to_file(local_path, 'execute_ctr_score_flow_plus - Success')


# OPS TABLE
def create_hdfs_location(local_path, hdfs_ops_location, verbose, cancel_execution_if_errors=1):
    cmd = ("hadoop fs -mkdir -p %s" % hdfs_ops_location)

    if verbose:
        msg = ("create_hdfs_location - Executing '%s'" % (cmd))
        log_to_file(local_path, msg)

    try:
        status, output = commands.getstatusoutput(cmd)

        if status != 0:
            raise RuntimeError("error %s" % (output))
    except:
        msg = ("create_hdfs_location - %s" % (sys.exc_info()[1]))
        log_to_file(local_path, msg, 'error')

        if cancel_execution_if_errors:
            msg = 'create_hdfs_location - Cannot create_hdfs_location - End execution'
            log_to_file(local_path, msg, 'error')
            # send_mail(msg)
            sys.exit(-1)
    else:
        log_to_file(local_path, 'create_hdfs_location - Success')


def load_csv_file(local_path, hdfs_ops_location, archive, verbose, cancel_execution_if_errors=1):
    data_location = ("{path}/data".format(path=local_path))

    cmd = ("hadoop fs -copyFromLocal -f %s/%s_%s.csv %s" % (data_location, SCORE_flow_FILE, archive, hdfs_ops_location))

    if verbose:
        msg = ("load_csv_file - Executing '%s'" % (cmd))
        log_to_file(local_path, msg)

    try:
        status, output = commands.getstatusoutput(cmd)

        if status != 0:
            raise RuntimeError("error %s" % (output))

    except:
        msg = ("load_csv_file - %s" % (sys.exc_info()[1]))
        log_to_file(local_path, msg, 'error')

        if cancel_execution_if_errors:
            msg = 'load_csv_file - Cannot load csv file to ops location - End execution'
            log_to_file(local_path, msg, 'error')
            sys.exit(-1)
    else:
        log_to_file(local_path, 'load_csv_file - Load csv file to ops location Success')


# STAGING - Parquet table

# def create_ops_hive_table(db,table,cancel_execution_if_errors=0):
def create_hive_table(hql_script, local_path, archive, verbose, hdfs_ops_location='', cancel_execution_if_errors=1):
    beeline = f"beeline -u 'jdbc:hive2://node002.eis.company.com:10000/;principal=hive/_HOST@EDA.company.COM' -f {local_path}/hqls/{hql_script}.sql"
    logs = (">>{local_path}/logs/beeline.log 2>>{local_path}/logs/beeline.err".format(local_path=local_path))

    if 'ops' in hql_script:
        cmd = f"{beeline} --hivevar DEST_DB={opsDef} --hivevar LOCATION={hdfs_ops_location} {logs}"

    elif 'staging' in hql_script:
        cmd = f"{beeline} --hivevar SOURCE_DB={opsDef} --hivevar DEST_DB={stagingDef} --hivevar ARCHV={archive} {logs}"

    elif ('prod' and 'weekly') or ('prod' and 'monthly') in hql_script:
        cmd = f"{beeline} --hivevar SOURCE_DB={stagingDef} --hivevar DEST_DB={prodDB} --hivevar ARCHV={archive} {logs}"

    elif 'prod' and 'atr' in hql_script:
        cmd = f"{beeline} --hivevar SOURCE_DB={stagingDef} --hivevar DEST_DB={prodDB} {logs}"

    elif 'prod' in hql_script:
        # Extract YYYYMM from archive (discard DD)
        global period
        period = int(str(archive)[:6])

        cmd = f"{beeline} --hivevar SOURCE_DB={stagingDef} --hivevar DEST_DB={prodDB} --hivevar ARCHV={period} {logs}"
    else:
        msg = f"create_hive_table - Cannot create_hive_table({hql_script}) - Not valid 'hql_script'"

        log_to_file(local_path, msg, 'error')
        sys.exit(msg)
    if verbose:
        msg = ("create_hive_table({hql_script}) - Executing '{cmd}'".format(hql_script=hql_script, cmd=cmd))
        log_to_file(local_path, msg)
    else:
        msg = ("create_hive_table({hql_script}) - Executing".format(hql_script=hql_script))
        log_to_file(local_path, msg)

    try:
        status, output = commands.getstatusoutput(cmd)

        if status != 0:
            raise RuntimeError(output)
    except:
        msg = ("create_hive_table(%s) - BEELINE ERROR - %s" % (hql_script, sys.exc_info()[1]))
        log_to_file(local_path, msg, 'error')

        if cancel_execution_if_errors:
            msg = ('create_hive_table - Cannot create_hive_table({hql_script}) - End execution'.format(
                hql_script=hql_script))
            log_to_file(local_path, msg, 'error')
            # send_mail(msg,local_path)
            sys.exit(-1)
    else:
        log_to_file(local_path, 'create_hive_table({hql_script}) - Success'.format(hql_script=hql_script))


def invalidate_metadata(db, table, local_path, verbose):
    cmd = f"beeline -u 'jdbc:hive2://node004.eis.company.com:21050/;principal=impala/node004@EDA.company.COM' -e " \
          f"'invalidate metadata {db}.{table};' >>{local_path}/logs/beeline.log 2>>{local_path}/logs/beeline.err"

    if verbose:
        msg = ("invalidate_metadata - Executing '{cmd}'".format(cmd=cmd))
        log_to_file(local_path, msg)

    try:
        status, output = commands.getstatusoutput(cmd)

        if status != 0:
            raise RuntimeError()
    except:
        msg = f"invalidate_metadata - Cannot invalidate metadata of '{db}.{table}' - {output}"
        log_to_file(local_path, msg, 'error')
    # send_mail(msg)
    else:
        log_to_file(local_path, f'invalidate_metadata - invalidate_metadata({db}.{table}) Success')


# PRODUCTION				
# If 'archive' next week change month => Execute monthly ingest	
def monthly_execution(archive):
    present_archive = datetime.strptime(str(archive), '%Y%m%d')
    seven_days_forward = present_archive + timedelta(7)

    if present_archive.month == seven_days_forward.month:
        return False
    else:
        return True


# Detect last Saturday (this will depend of the definition, i.e. could be: Monday,Tuesday,Wednesday,Thursday,etc)
def archive_func(fst, weekday_to_execute='Saturday'):
    import datetime

    d = datetime.date.today()

    if fst:
        return int(d.strftime("%Y%m01"))
    else:
        while d.strftime("%A") != weekday_to_execute:
            # print ("No es lunes: %s" %d.strftime("%Y-%m-%d --> %A %B"))
            d -= datetime.timedelta(1)

        return int(d.strftime("%Y%m%d"))


def last_ingestion(db, first, archive, local_path, verbose):
    now = datetime.now()

    if first:
        # sql = ("SELECT time_stamp FROM {db}.ctr_comp_indicadores_weekly where archive={archive} LIMIT 1;" .format(db=db,archive=archive))
        sql = f"SELECT fecha_obs FROM {db}.ctr_comp_indicadores_weekly where archive={archive} LIMIT 1;"
    else:
        # sql = ("SELECT time_stamp FROM {db}.ctr_comp_indicadores LIMIT 1;" .format(db=db))
        sql = ("SELECT fecha_obs FROM {db}.ctr_comp_indicadores LIMIT 1;".format(db=db))
    # sql = ("SELECT fecha_obs FROM ctr_comp_prod.ctr_comp_indicadores LIMIT 1;")

    beeline = f"beeline -u 'jdbc:hive2://node004.eis.company.com:21050/;principal=impala/node004@EDA.company.COM' " \
              f"--showHeader=false --outputformat=dsv --silent=true -e '{sql}'"
    logs = ("2>>{local_path}/logs/beeline.err".format(local_path=local_path))

    cmd = ("{beeline} {logs}".format(beeline=beeline, logs=logs))

    if verbose:
        msg = ("last_ingestion - Executing '%s'" % cmd)
        log_to_file(local_path, msg)
    try:
        status, output = commands.getstatusoutput(cmd)

        if status != 0:
            raise RuntimeError()
    except:
        msg = ("last_ingestion - Cannot check last_ingestion of 'ctr_comp_prod.ctr_comp_indicadores' - %s" % (output))
        log_to_file(local_path, msg, 'error')
        # send_mail(msg,local_path)
        sys.exit(msg)
    else:
        if output:
            # aux = datetime.strptime(output, '%Y-%m-%d %H:%M:%S')		#With time_stamp
            aux = datetime.strptime(output, '%Y-%m-%d')
        else:  # If no output, means there is nothing in the table (usually when using 'first')
            aux = datetime.strptime('2019-01-01 00:00:00', '%Y-%m-%d')

        dif = str(now - aux)

        if "days" not in dif:
            dias = 0
        else:
            dias = dif.split(' ')[0]

        log_to_file(local_path, 'last_ingestion() - Success')
        return int(dias)


def check_partitions(local_path, db, table):
    # TEST- DEBUG
    # db='ar_comp_prod'
    # table='ar_comp_score_3_atr_monthly'

    hql = ("show partitions %s.%s;" % (db, table))
    cmd = f"beeline -u 'jdbc:hive2://node002.eis.company.com:10000/;principal=hive/_HOST@EDA.company.COM' " \
          f"--showHeader=false --outputformat=dsv --silent=true -e {hql} 2>>{local_path}/logs/beeline.err"

    try:
        status, output = commands.getstatusoutput(cmd)

        if status != 0:
            raise RuntimeError(output)
    except:
        msg = ("check_partitions(%s) - BEELINE ERROR - %s" % (hql, sys.exc_info()[1]))
        log_to_file(local_path, msg, 'error')

    else:
        aux = output.split('\n')

        number_of_partitions = len(aux)
        # Candidate partition to delete if limit was found.
        partitions = aux

        # print "CANTIDAD DE PARTICIONES: %i " %number_of_partitions
        # print "Particion mas vieja: " + first_partition

        log_to_file(local_path, 'check_partitions(%s) - Success' % (hql))
        return number_of_partitions, partitions


def hive_execute(hql_script, db, table, part2remove, local_path, verbose):
    beeline = f"beeline -u 'jdbc:hive2://node002.eis.company.com:10000/;principal=hive/_HOST@EDA.company.COM' " \
              f"-f {local_path}/hqls/{hql_script}.sql"

    cmd = f"{beeline} --hivevar DB={db} --hivevar TABLE={table} --hivevar PARTITION_TO_REMOVE={part2remove} " \
          f">>{local_path}/logs/beeline.log 2>>{local_path}/logs/beeline.err"

    if verbose:
        msg = ("hive_execute({hql_script}) - Executing '{cmd}'".format(hql_script=hql_script, cmd=cmd))
        log_to_file(local_path, msg)
    else:
        msg = (
            "hive_execute({hql_script}) on {db}.{table} - Executing".format(hql_script=hql_script, db=db, table=table))
        log_to_file(local_path, msg)

    try:
        status, output = commands.getstatusoutput(cmd)

        if status != 0:
            raise RuntimeError(output)
    except:
        msg = ("hive_execute(%s) - BEELINE ERROR - %s" % (hql_script, sys.exc_info()[1]))
        log_to_file(local_path, msg, 'error')
    else:
        log_to_file(local_path, 'hive_execute(%s) - Success' % (hql_script))


# part2remove: if not provided, will use data for current table
def hadoop_execute(local_path, table_hdfs_location, db, table, purgeFolder, verbose, part2remove='data/'):
    cmd = f"hadoop fs -mkdir -p {purgeFolder}"

    if verbose:
        msg = ("Creating purge destination in HDFS - %s" % cmd)
        log_to_file(local_path, msg)
    else:
        if 'data' in part2remove:
            msg = "Creating purge destination in HDFS"
            log_to_file(local_path, msg)
        else:
            msg = ("Creating purge destination in HDFS - partition '%s'" % part2remove)
            log_to_file(local_path, msg)

    # Not catching error message yet
    # status, output = commands.getstatusoutput(cmd)

    cmd = f"hadoop fs -mv {table_hdfs_location}{part2remove} {purgeFolder}"

    if verbose:
        msg = ("hadoop_execute - Executing '%s'" % (cmd))
        log_to_file(local_path, msg)
    else:
        msg = ("hadoop_execute - moving %s data - Executing" % part2remove)
        log_to_file(local_path, msg)

    try:
        status, output = commands.getstatusoutput(cmd)

        if status != 0:
            raise RuntimeError(output)
    except:
        msg = ("hadoop_execute - 'hadoop fs -mv' - %s" % (sys.exc_info()[1]))
        log_to_file(local_path, msg, 'warning')
    else:
        log_to_file(local_path, 'hadoop_execute -mv- Success')


def purge_table(local_path, db, table, archive, verbose, partitions_to_retain=0, partitioned_table=True):
    """
    OBS: para hacer un purge en una particionada verificar estos pasos:
    HIVE:
        ((insert nueva particion))
        chequear si hay que borrar (lo hace esta func)
        hive:   drop partition vieja
        hadoop: mv archive de particion vieja a zona purge

    En tabla no particionada:
        mv data vieja a zona de purge
        ((insert overwrite))
    """

    if 'ctr_comp_score_flow_plus' == table:
        tableFolder = table + '/data/'
    elif 'ctr_comp_score_flow_plus_atr' == table:
        # tableFolder = 'ctr_comp_score_flow_plus_atr/current/data/'
        tableFolder = 'ctr_comp_score_flow_plus_atr/current/'
    elif 'ctr_comp_score_flow_plus_atr_weekly' == table:
        tableFolder = 'ctr_comp_score_flow_plus_atr/weekly/data/'
    elif 'ctr_comp_score_flow_plus_atr_monthly' == table:
        tableFolder = 'ctr_comp_score_flow_plus_atr/monthly/data/'

    table_hdfs_location = PROD_HDFS_FOLDER + '/' + tableFolder

    # For production
    # purgeBaseFolder = "/user/cluster_name/purge/production/score_flow/" + tableFolder

    # For dev:
    # Tabla current:
    purgeBaseFolder = PROD_HDFS_FOLDER + "/purge/archive=" + str(archive) + '/' + tableFolder

    if partitioned_table:
        total_number_of_partitions, partitions_list = check_partitions(local_path, db, table)

        dif = total_number_of_partitions - partitions_to_retain
        msg = ("purge_table - %s.%s - Current amount of partitions to remove is %i" % (db, table, dif))
        log_to_file(local_path, msg)

        while dif > 0:
            partition_to_remove = partitions_list.pop(0)
            hive_execute('drop_partition', prodDB, table, partition_to_remove)
            hadoop_execute(local_path, table_hdfs_location, prodDB, table, purgeBaseFolder, verbose,
                           partition_to_remove)

            dif -= 1

        else:
            msg = ("purge_table - No (more) partition to remove in '%s.%s'" % (db, table))
            log_to_file(local_path, msg)
    else:
        msg = ("purge_table - Not partitioned table - moving '%s.%s' data to purge zone" % (db, table))
        log_to_file(local_path, msg)
        hadoop_execute(local_path, table_hdfs_location, prodDB, table, purgeBaseFolder, verbose)
