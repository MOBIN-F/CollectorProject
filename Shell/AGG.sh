#!/bin/bash
#MR,DPI
hour=$1
day=${hour:0:8}
((hadoop fs -du /DATA/PUBLIC/NOCE/AGG/AGG_MRO_CHR_RELATE/day=${day}/hour=${hour}/vendor=*/ || true) \
&& hadoop fs -du /DATA/PUBLIC/NOCE/AGG/AGG_EVT_LTE_DPI_NEW/hour=${hour})|
      awk -v hour=${hour} -F '[ =/]+' '
          BEGIN{
             HW_dirSize=0;         #文件总大小
             ZTE_dirSize=0;
             ERS_dirSize=0;
             DPI_dirSize=0;

             ERS_LZOCount=0;        #含LZO文件的个数
             HW_LZOCount=0;
             ZTE_LZOCount=0;
             DPI_LZOCount=0;

             isInvalidDir_HW="false";     #是否含有目录
             isTxtFile_HW="false"; #是否有文件未进行压缩
             isInvalidDir_ERS="false";
             isTxtFile_ERS="false";
             isInvalidDir_ZTE="false";
             isTxtFile_ZTE="false";
             isInvalidDir_DPI="false";
             isTxtFile_DPI="false";
          }
          {
            if($7 == "AGG_MRO_CHR_RELATE"){
             if(match($14,"lzo") && !match($14,"lzo.index")){
                $13 == "HW" ? fun(HW_dirSize+=$1,HW_LZOCount++) : ($13 == "ZTE" ? fun(ZTE_dirSize+=$1,ZTE_LZOCount++) : fun(ERS_dirSize+=$1,ERS_LZOCount++))
              }else if(match($14,"txt") && !match($14,"lzo.index")){
                $13 == "HW" ? invalid(isTxtFile_HW="true") : ($13 == "ZTE" ? invalid(isTxtFile_ZTE="true") : invalid(isTxtFile_ERS="true"))
              }else if(match($14,"^[0-9]+")){  #判断是否有纯数字名目录
                $13 == "HW" ? invalid(isInvalidDir_HW="true") : ($13 == "ZTE" ? invalid(isInvalidDir_ZTE="true") : invalid(isInvalidDir_ERS="true"))
              }
            }else if($7 == "AGG_EVT_LTE_DPI_NEW"){
             if(match($10,"lzo") && !match($10,"lzo.index")){
               fun(DPI_dirSize+=$1,DPI_LZOCount++)
              }else if(match($10,"txt") && !match($10,"lzo.index")){
                isTxtFile_DPI="true"
              }else if(match($10,"^[0-9]+")){  #判断是否有纯数字名目录
                isInvalidDir_DPI="true";
              }
            }
          }
          END{
             _printf("HW_MR",hour,HW_LZOCount,HW_dirSize,isInvalidDir_HW,isTxtFile_HW) 
             _printf("ZTE_MR",hour,ZTE_LZOCount,ZTE_dirSize,isInvalidDir_ZTE,isTxtFile_ZTE)
             _printf("ERS_MR",hour,ERS_LZOCount,ERS_dirSize,isInvalidDir_ERS,isTxtFile_ERS)
             _printf("DPI",hour,DPI_LZOCount,DPI_dirSize,isInvalidDir_DPI,isTxtFile_DPI)
                
          }
          function fun(size,count){}
          function invalid(size){ }
          function _printf(table,timestamp,count,size,isInvalidDir,isTxtFile){
             #printf ("AGG.%s.LZOCount\t%s\t%s\t%s_dirSize=%f\tisInvalidDir=%s\tisTxtFile=%s\n",table,timestamp,count,table,size/(1024^3),isInvalidDir,isTxtFile)
             printf ("AGG.%s.LZOCount\t%s\t\033[1;31;40m %s\033[0m\t%s_dirSize=%f\t\tdir=%s\ttxt=%s\n",table,timestamp,count,table,size/(1024^3),isInvalidDir,isTxtFile)
          }'