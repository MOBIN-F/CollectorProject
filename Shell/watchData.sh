#!/bin/bash
path[0]="/DATA/PUBLIC/NOCE/ETL/ETL_CHR_L_MM/"$1
path[1]="/DATA/PUBLIC/NOCE/ETL/ETL_4G_MRO_ZTE/"$1
path[2]="/DATA/PUBLIC/NOCE/ETL/ETL_4G_MRO_ERS/"$1
path[3]="/DATA/PUBLIC/NOCE/ETL/ETL_4G_MRO_HW/"$1
path[4]="/DATA/PUBLIC/NOCE/ETL/ETL_HW_DNS/"$1
path[5]="/DATA/PUBLIC/NOCE/ETL/ETL_HW_HTTP_Browsing/"$1
path[6]="/DATA/PUBLIC/NOCE/ETL/ETL_HW_Other/"$1
path[7]="/DATA/PUBLIC/NOCE/ETL/ETL_HW_Straming/"$1
path[8]="/DATA/PUBLIC/NOCE/ETL/ETL_ZTE_CDT/"$1
path[9]="/DATA/PUBLIC/NOCE/ETL/ETL_HW_LTE_CDR/"$1
path[10]="/DATA/PUBLIC/NOCE/AGG/AGG_MRO_CHR_RELATE/day="$1
path[11]="-s /DATA/PUBLIC/NOCE/AGG/AGG_EVT_LTE_DPI_NEW/hour="$1*
path[12]="/DATA/PUBLIC/NOCE/ETL/ETL_LTEUP_*/"$1

length=${#path[*]}
for((index=0; index<length; index++)){
hadoop fs -du  ${path[index]} | awk -F '[/=]' '{print $1,$6,$10,$8}' | awk -F ' ' '{printf "%s,%s,%.4f\n",$3,$4,$1/(1024^3)}' >> watchData$1.csv
}
java -jar CSVToExcel-with-dependencies.jar watchData$1.csv
echo "Finished!"
exit 0
