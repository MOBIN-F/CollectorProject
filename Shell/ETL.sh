#!/bin/bash
#CHR,MR,DPI,CDR
if [ -z $1 ];then
echo "ERROR!"
echo "Usage:sh ETL.sh time"
exit 1
else
hour=$1
day=${hour:0:8}
((hadoop fs -du /DATA/PUBLIC/NOCE/ETL/ETL_4G_MRO_*/${day}/${hour} || true) \
&& (hadoop fs -du /DATA/PUBLIC/NOCE/ETL/ETL_CHR_L_MM/${day}/${hour} || true) \
&& (hadoop fs -du /DATA/PUBLIC/NOCE/ETL/ETL_HW_DNS/${day}/${hour} || true) \
&& (hadoop fs -du /DATA/PUBLIC/NOCE/ETL/ETL_HW_LTE_CDR/${day}/${hour} || true) \
&& (hadoop fs -du /DATA/PUBLIC/NOCE/ETL/ETL_HW_Straming/${day}/${hour} || true) \
&& (hadoop fs -du /DATA/PUBLIC/NOCE/ETL/ETL_HW_Other/${day}/${hour} || true) \
&& (hadoop fs -du /DATA/PUBLIC/NOCE/ETL/ETL_HW_Other/${day}/${hour} || true)
)|
     awk -F '[ =/]+' '
          BEGIN{
             HW_dirSize=0;         #文件总大小
             ZTE_dirSize=0;
             ERS_dirSize=0;
             CHR_dirSize=0;
             CDR_dirSize=0;
             CDT_dirSize=0;
             DNS_dirSize=0;
             Streaming_dirSize=0;
             Other_dirSize=0;

             Other_LzoCount=0;
             Streaming_LzoCount=0;
             DNS_LzoCount=0;
             ERS_LzoCount=0;        #含LZO文件的个数
             HW_LzoCount=0;
             ZTE_LzoCount=0;
             CHR_LzoCount=0;
             CDR_LzoCount=0;
             CDT_LzoCount=0;

             isInvalidDir_HW="false";     #是否含有目录
             isInvalidDir_ZTE="false";
             isInvalidDir_ERS="false";
             isInvalidDir_CHR="false";
             isInvalidDir_DNS="false";
             isInvalidDir_CDR="false";
             isInvalidDir_CDT="false";
             isInvalidDir_Other="false";
             isInvalidDir_Streaming="false";

             isTxtFile_HW="false"; #是否有文件未进行压缩
             isTxtFile_ZTE="false";
             isTxtFile_ERS="false";
             isTxtFile_CHR="false";
             isTxtFile_DNS="false";
             isTxtFile_CDR="false";
             isTxtFile_CDT="false";
             isTxtFile_Other="false";
             isTxtFile_Streaming="false";
          }
          {

            if(match($10,"lzo") && !match($10,"lzo.index")){  #统计lzo文件数目及总大小
                $7 == "ETL_4G_MRO_HW" ? fun(HW_dirSize+=$1,HW_LzoCount++) :
                  ($7 == "ETL_HW_LTE_CDR"?fun(CDR_dirSize+=$1,CDR_LzoCount++) :
                     ($7 == "ETL_4G_MRO_ZTE" ? fun(ZTE_dirSize+=$1,ZTE_LzoCount++) :
                       ($7 == "ETL_4G_MRO_ERS" ? fun(ERS_dirSize+=$1,ERS_LzoCount++) :
                          ($7 == "ETL_HW_LTE_CDR"?fun(CDR_dirSize+=$1,CDR_LzoCount++) :
                            ($7 == "ETL_HW_Straming" ? fun(Streaming_dirSize+=$1,Streaming_LzoCount++):
                               ($7 == "ETL_HW_DNS" ? fun(DNS_dirSize+=$1,DNS_LzoCount++) :
                                  ($7 == "ETL_CHR_L_MM" ?fun(CHR_dirSize+=$1,CHR_LzoCount++):
                                      fun(Other_dirSize+=$1,Other_LzoCount++))))))))
              }else if((match($10,"txt") && !match($10,"lzo.index"))){  #判断是不否存在未压缩文件
                 $7 == "ETL_4G_MRO_HW" ? Invalid(isTxtFile_HW="true") :
                    ($7 == "ETL_4G_MRO_ZTE" ? Invalid(isTxtFile_ZTE="true") :
                       ($7 == "ETL_4G_MRO_ERS" ? Invalid(isTxtFile_ERS="true") :
                          ($7 == "ETL_HW_LTE_CDR"?Invalid(isTxtFile_CDR="true") :
                              ($7 == "ETL_HW_Straming" ? Invalid(isTxtFile_Streaming="true"):
                                 ($7 == "ETL_HW_DNS" ? Invalid(isTxtFile_DNS="true"):
                                    ($7 == "ETL_CHR_L_MM" ? Invalid(isTxtFile_CHR="true"):
                                       Invalid(isTxtFile_Other="true")))))))
              }else if(match($10,"^[0-9]+")){  #判断是否有纯数字名目录
              print("0-9")
                $7 == "ETL_4G_MRO_HW" ? Invalid(isInvalidDir_HW="true") :
                    ($7 == "ETL_4G_MRO_ZTE" ? Invalid(isInvalidDir_ZTE="true") :
                       ($7 == "ETL_4G_MRO_ERS" ? Invalid(isInvalidDir_ERS="true") :
                          ($7 == "ETL_HW_LTE_CDR"?Invalid(isInvalidDir_CDR="true") :
                             ($7 == "ETL_HW_Straming" ? Invalid(isInvalidDir_Streaming="true"):
                                 ($7 == "ETL_HW_DNS"?Invalid(isInvalidDir_DNS="true"):
                                     ($7 == "ETL_CHR_L_MM" ? Invalid(isInvalidDir_CHR="true"):
                                         Invalid(isInvalidDir_Other="true")))))))
              }
              tableName=$7
              timestamp=$9


          }
          END{
              _printf("HW_MR",timestamp,HW_LzoCount,HW_dirSize,isInvalidDir_HW,isTxtFile_HW)
              _printf("ZTE_MR",timestamp,ZTE_LzoCount,ZTE_dirSize,isInvalidDir_ZTE,isTxtFile_ZTE)
              _printf("ERS_MR",timestamp,ERS_LzoCount,ERS_dirSize,isInvalidDir_ERS,isTxtFile_ERS)
              _printf("CHR",timestamp,CHR_LzoCount,CHR_dirSize,isInvalidDir_CHR,isTxtFile_CHR)
              _printf("HW_CDR",timestamp,CDR_LzoCount,CDR_dirSize,isInvalidDir_CDR,isTxtFile_CDR)
              _printf("ZTE_CDT",timestamp,CDT_LzoCount,CDT_dirSize,isInvalidDir_CDT,isTxtFile_CDT)
              _printf("DNS",timestamp,DNS_LzoCount,DNS_dirSize,isInvalidDir_DNS,isTxtFile_DNS)
              _printf("Other",timestamp,Other_LzoCount,Other_dirSize,isInvalidDir_Other,isTxtFile_Other)
              _printf("Streaming",timestamp,Streaming_LzoCount,Streaming_dirSize,isInvalidDir_Streaming,isTxtFile_Streaming)
          }

          function fun(size,count){}
          function Invalid(invalidDir){}
          function _printf(table,timestamp,count,size,isInvalidDir,isTxtFile){
           printf ("ETL.%s.LZOCount\t%s\t%s\t%s_dirSize=%f'G'\tDir=%s\ttxt=%s\n",table,timestamp,count,table,size/(1024^3),isInvalidDir,isTxtFile)
          }' | awk '$0~"ETL"'
fi
exit 0