#/bin/bash
path[0]=ETL_CHR_L_MM
path[1]=ETL_4G_MRO_ERS
path[2]=ETL_4G_MRO_HW
path[3]=ETL_4G_MRO_ZTE
path[4]=ETL_ZTE_CDT
path[5]=ETL_HW_LTE_CDR
length=${#path[*]}
hour=$1
day=${hour:0:8}
d="^[0-9]{10}$"
i=1
if [ "$#" -eq "1" ] && [ "${#hour}" -eq "10" ] && [[ "$hour" =~ $d ]];then
   echo -n "是否确定删除"
   echo -en "\\033[31;40m" "${hour:0:4}-${hour:4:2}-${hour:6:2}:${hour:8:2}"
   echo -en "\\033[0;40m" "下的数据:"
    while [ $i -le 3  ]
	do 
       read -p "(Y/N)" check;      
	    case $check in
	       (Y|y)
		      for((index=0;index<length;index ++)){
                 echo "/DATA/PUBLIC/NOCE/ETL/${path[index]}/${day}/${hour}"
              }
			  echo "数据已成功删除!"
	          break;;
		   (N|n)
              echo "已取消删除操作!"
	          exit 0;;
		   *)echo "错误的指令!"		
	          ((i++))
	        continue;
	    esac
	done
else
echo "ERROR!"
echo "Usage:sh XXX.sh yyyymmddhh"
exit 1
fi
