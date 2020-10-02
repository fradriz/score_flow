#!/usr/bin/python
# -*- coding: utf-8 -*-

#########################################################################
# Script to load Py Score flow Positiva (Plus) data into Hive db        #
#                                                                       #
# This program is to automate the ingestion.                            #
# Developer: Facundo Radrizzani.                                         #
#########################################################################

import os
from score_faja.ext_functions import *


def main():
    parameters = input_parameters()

    # for key,value in parameters.iteritems():
    #   print "CLAVE:\t{}\t\t\tVALOR:{}" .format(key,value)

    archive = parameters['archive']
    opsDef = parameters['ops_db']
    prodDB = parameters['prod_db']

    check_precedence = parameters['precedence']
    r_script = parameters['r_script']
    verbose = parameters['verbose']
    first = parameters['first']

    OPS_HDFS_FOLDER = '/user/cluster_name/' + opsDef + '/comp/country/score_flow_plus'

    msg = f"#------------------------------ SCORE flow (+) - START - ARCHIVE={archive} -------------------------------#"
    log_to_file(EDGE_NODE_LOCATION, msg)
    log_to_file(EDGE_NODE_LOCATION, parameters)

    # 0) Check if flow was executed. Useful to set many days in a row in crontab (ie from Sat to Tue same time).
    # Looks for file like this: score_flow_plus_20190511.EXECUTE
    file_to_check = f"{EDGE_NODE_LOCATION}/logs/{SCORE_flow_FILE}_{archive}.EXECUTED"

    if os.path.isfile(file_to_check):
        msg = f"Flow for archive={archive} was already executed - not executing it again"
        log_to_file(EDGE_NODE_LOCATION, msg)
        sys.exit(msg)

    # 1) If ERS password change (it does every two months), renew kerberos key 1.i) Kerberos auth. Needed before
    # connecto to Hive or Impala. It will cancel execution and send email if can't authenticate
    kerberos_auth(EDGE_NODE_LOCATION, verbose, cancel_execution_if_errors=1)

    # 1.ii) This step is neccesary to avoid problems extracting the data in the R script using impala.
    invalidate_metadata(prodDB, 'ctr_comp_indicadores', EDGE_NODE_LOCATION, verbose)

    # 1.iii) Precedence: Flow must execute after ctr_comp_indicadores was ingested. Can be disabled.
    if check_precedence:
        msg = "Checking precedence - last time ctr_comp_indicadores was ingested?"
        log_to_file(EDGE_NODE_LOCATION, msg)

        number_of_days = last_ingestion(prodDB, first, archive, EDGE_NODE_LOCATION, verbose)

        if number_of_days < 4:
            msg = f"Checking precedence - OK - '{prodDB}.ctr_comp_indicadores' was ingested {number_of_days} days ago"
            log_to_file(EDGE_NODE_LOCATION, msg)
        else:
            msg = f"Checking precedence - NOK - '{prodDB}.ctr_comp_indicadores' was ingested {number_of_days} days ago"
            log_to_file(EDGE_NODE_LOCATION, msg, 'error')
            send_mail(EDGE_NODE_LOCATION, msg, archive)
            sys.exit(msg)
    else:
        msg = "Not checking precedence"
        log_to_file(EDGE_NODE_LOCATION, msg)

    # 2) Execute R script
    # global data_location
    # data_location=("{path}/data" .format(path=EDGE_NODE_LOCATION))
    execute_ctr_score_flow(EDGE_NODE_LOCATION, prodDB, r_script, archive, verbose, cancel_execution_if_errors=1)

    # 3) OPS - HDFS & HIVE Table
    # 3.i) Create ops_hdfs_location
    hdfs_ops_location = (
        "{OPS_HDFS_FOLDER}/data/archive={archive}".format(OPS_HDFS_FOLDER=OPS_HDFS_FOLDER, archive=archive))
    create_hdfs_location(EDGE_NODE_LOCATION, hdfs_ops_location, verbose)

    # 3.ii) Upload new csv file to ops location
    load_csv_file(EDGE_NODE_LOCATION, hdfs_ops_location, archive, verbose, cancel_execution_if_errors=1)

    # 3.iii) Create ops table
    create_hive_table('ops_score_flow_plus_table_atr', EDGE_NODE_LOCATION, archive, verbose, hdfs_ops_location)

    # 4) STAGING
    create_hive_table('staging_score_flow_plus_table_atr', EDGE_NODE_LOCATION, archive, verbose)

    # 5) PRODUCTION
    # 5.i) CURRENT
    purge_table(EDGE_NODE_LOCATION, prodDB, 'ctr_comp_score_flow_plus_atr', archive, verbose, partitioned_table=False)

    create_hive_table('prod_score_flow_plus_table_atr', EDGE_NODE_LOCATION, archive, verbose)
    invalidate_metadata(prodDB, 'ctr_comp_score_flow_plus_atr', EDGE_NODE_LOCATION, verbose)

    # Partitioned table with only score & flow data (not attributes)
    create_hive_table('prod_score_flow_plus_table', EDGE_NODE_LOCATION, archive, verbose)

    purge_table(EDGE_NODE_LOCATION, prodDB, 'ctr_comp_score_flow_plus', archive, verbose, score_flow_PartitionsToRetain)

    invalidate_metadata(prodDB, 'ctr_comp_score_flow_plus', EDGE_NODE_LOCATION, verbose)

    # 5.ii) WEEKLY
    create_hive_table('prod_score_flow_plus_atr_table_weekly', EDGE_NODE_LOCATION, archive, verbose)

    purge_table(EDGE_NODE_LOCATION, prodDB, 'ctr_comp_score_flow_plus_atr_weekly', archive, verbose,
                weeklyPartitionsToRetain)

    invalidate_metadata(prodDB, 'ctr_comp_score_flow_plus_atr_weekly', EDGE_NODE_LOCATION, verbose)

    # 5.iii) MONTHLY
    if monthly_execution(archive):
        msg = 'Executing monthly'
        log_to_file(EDGE_NODE_LOCATION, msg)

        create_hive_table('prod_score_flow_plus_atr_table_monthly', EDGE_NODE_LOCATION, archive, verbose)

        purge_table(EDGE_NODE_LOCATION, prodDB, 'ctr_comp_score_flow_plus_atr_monthly', archive, verbose,
                    monthlyPartitionsToRetain)

        invalidate_metadata(prodDB, 'ctr_comp_score_flow_plus_atr_monthly', EDGE_NODE_LOCATION, verbose)
    else:
        msg = 'Not time for monthly execution - Skipping'
        log_to_file(EDGE_NODE_LOCATION, msg)

    # 6) Inform the flow with current archive was executed succesfully by saving .EXECUTED file in logs.
    msg = ("Flow was executed - Creating file '{file}'".format(file=file_to_check))
    log_to_file(EDGE_NODE_LOCATION, msg)
    open(file_to_check, 'a').close()

    msg = f"#------------------------------- SCORE flow (+) - END - ARCHIVE={archive} -------------------------------#"
    log_to_file(EDGE_NODE_LOCATION, msg)

# if __name__ == "__main__":
#   main()
