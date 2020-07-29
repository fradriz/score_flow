# Description
Python app to automate the running of an R script to score an audience. Using Hive SQL/Impala SQL to perform DB tasks.

# Usage
exe_ctr_score_flow.py [<-archive=YYYYMMDD>] [<-opsDB=ops_dev>] [<-stagingDB=staging_dev>] [<-prodDB=prod_dev>] [<-prodHDFS=prod_dev>] [<-verbose=false>] [<-send_mail=False>] [<-precedence=True>] [-h] [-v]
```
i.e.: python exe_ctr_score_flow.py -archive 20190101 -opsDB=ops_dev -verbose=true
i.e.: python exe_ctr_score_flow.py
i.e.: python exe_ctr_score_flow.py --prodDB prj_ctr_scores_db -send_mail true -verbose true
```

Flow to calculate and ingest score_flow into Hive tables

optional arguments:
```
  -h, --help            show this help message and exit
  -archive ARCHIVE      archive=YYYYMMDD. Default value will be last Monday.
  -opsDB OPS_DB         Options=[ops,ops_dev] Default=ops_dev.
  -stagingDB STAGING_DB
                        Options=[staging,staging_dev] Default=staging_dev.
  -prodDB PROD_DB       Options=[ctr_comp_prod,prj_ctr_scores_db,prod_dev]
                        Default=prod_dev.
  -prodHDFS PRODHDFS    Production HDFS name. Options[prod_dev, production]
  -precedence PRECEDENCE
                        Check if 'ctr_comp_prod.ctr_comp_indicadores' was ingested
                        this week.
  -send_mail SMAIL      Send notification mail [True,False].
  -verbose VERBOSE      Show more info in logs.[True,False].
  -v                    show program's version number and exit
```

# Prerequisits

Before execute the flow is neccesary to have the right kerberos credentials. To do it so follow this steps:

1) Neccesary only when ERS password change (every two months or so)
```
$ sudo -u sys_dev ktutil
ktutil:  addent -password -p sys_dev@ERS.company.COM -k 1 -e RC4-HMAC
Password for sys_dev@ERS.company.COM: ("sys_dev ERS password, must provide by hand")
ktutil:  wkt /export/home/sys_dev/sys_dev.keytab
ktutil:  q
```

This command is included in the flow:
* sudo -u sys_dev kinit sys_dev@ERS.company.COM -k -t /export/home/sys_dev/sys_dev.keytab

# Tables

Create prod tables:
* ctr_comp_score_flow_atr
* ctr_comp_score_flow_atr_weekly
* ctr_comp_score_flow_atr_monthly	
* ctr_comp_score_flow_atr
	

|BD          |TABLES                        | CONTENT                   |Format  |Frequency   |History*        |Tokenization |Partitioned
|------------|------------------------------|---------------------------|--------|------------|----------------|-------------|-------------
|ctr_comp_prod |ctr_comp_score_flow             |Score & flow               |Parquet |Weekly      |60 months (*1)  |Simple       | Yes
|ctr_comp_prod |ctr_comp_score_flow_atr         |Score & flow & Attributes  |Parquet |Weekly      |Last intake     |Simple       | No
|ctr_comp_prod |ctr_comp_score_flow_atr_weekly  |Score & flow & Attributes  |Parquet |Weekly      |Last 3 months   |Simple       | Yes
|ctr_comp_prod |ctr_comp_score_flow_atr_monthly |Score & flow & Attributes  |Parquet |Monthly(*2) |59 months       |Double       | Yes

```
* (# of archives)
(*1)
(*2)
```

# HDFS

TABLE                         | HDFS path                   
------------------------------|-------------------------------------------------------------------------------------------------------
ctr_comp_score_flow             |/user/cluster_name/prodHDFS/comp/country/score_flow/ctr_comp_score_flow/data/archive=<archive>               
ctr_comp_score_flow_atr         |/user/cluster_name/prodHDFS/comp/country/score_flow/ctr_comp_score_flow_atr/current/data  
ctr_comp_score_flow_atr_weekly  |/user/cluster_name/prodHDFS/comp/country/score_flow/ctr_comp_score_flow_atr/weekly/data/archive=<archive>   
ctr_comp_score_flow_atr_monthly |/user/cluster_name/prodHDFS/comp/country/score_flow/ctr_comp_score_flow_atr/monthly/data/archive=<archive>  

```
dev:
	prodHDFS=prod_dev
production:
	prodHDFS=ctr_comp_prod
```