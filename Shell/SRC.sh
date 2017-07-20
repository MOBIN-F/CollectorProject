#!/bin/bash
#CHR,MR,DPI,CDR
hour=$1
day=${hour:0:8}
((hadoop fs -du /DATA/PUBLIC/NOCE/SRC/SRC_4G_MRO_*/${day}/${hour} || true) \
&& (hadoop fs -du /DATA/PUBLIC/NOCE/SRC/SRC_CHR_L_MM/${day}/${hour} || true) \
&& (hadoop fs -du /DATA/PUBLIC/NOCE/SRC/SRC_HW_Other/${day}/${hour} || true) \
&& (hadoop fs -du /DATA/PUBLIC/NOCE/SRC/SRC_HW_LTE_CDR/${day}/${hour} || true) \
&& (hadoop fs -du /DATA/PUBLIC/NOCE/SRC/SRC_HW_Straming/${day}/${hour} || true) \
&& (hadoop fs -du /DATA/PUBLIC/NOCE/SRC/SRC_HW_DNS/${day}/${hour} || true) )|
     awk -F '[ =/]+' '
          BEGIN{
             HW_dirSize=0;         #文件总大小
             ZTE_dirSize=0;
             ERS_dirSize=0;
             CHR_dirSize=0;
             CDR_dirSize=0;
             DNS_dirSize=0;
             Streaming_dirSize=0;
             Other_dirSize=0;

             Other_Count=0;
             Streaming_Count=0;
             DNS_Count=0;
             ERS_Count=0;        #含LZO文件的个数
             HW_Count=0;
             ZTE_Count=0;
             CHR_Count=0;
             CDR_Count=0;
       
          }
          {
                $7 == "SRC_4G_MRO_HW" ? fun(HW_dirSize+=$1,HW_Count++) : 
                    ($7 == "SRC_4G_MRO_ZTE" ? fun(ZTE_dirSize+=$1,ZTE_Count++) : 
                       ($7 == "SRC_4G_MRO_ERS" ? fun(ERS_dirSize+=$1,ERS_Count++) : 
                          ($7 == "SRC_HW_LTE_CDR" ? fun(CDR_dirSize+=$1,CDR_Count++) : 
                              ($7 == "SRC_HW_Straming" ? fun(Streaming_dirSize+=$1,Streaming_Count++): 
                                  ($7 == "SRC_HW_Other" ? fun(Other_dirSize+=$1,Other_Count++): 
                                     ($7 == "SRC_HW_DNS" ? fun(DNS_dirSize+=$1,DNS_Count++): 
                                         fun(CHR_dirSize+=$1,CHR_Count++)))))))    

              tableName=$7
              timestamp=$9           
          }
          END{
              _printf("HW_MR",timestamp,HW_Count,HW_dirSize)
              _printf("ZTE_MR",timestamp,ZTE_Count,ZTE_dirSize)
              _printf("ERS_MR",timestamp,ZTE_Count,ERS_dirSize)
              _printf("CHR_MR",timestamp,CHR_Count,CHR_dirSize)
              _printf("CDR_MR",timestamp,CDR_Count,CDR_dirSize)
              _printf("DNS_MR",timestamp,DNS_Count,DNS_dirSize)
              _printf("Other",timestamp,Other_Count,Other_dirSize)
              _printf("Streaming",timestamp,Streaming_Count,Streaming_dirSize)                
          }
          
          function fun(size,count){}
          function Invalid(invalidDir){}
          function _printf(table,timestamp,count,size){
           printf ("SRC.%s.LZOCount %s %s %s_dirSize=%f'G'\n",table,timestamp,count,table,size/(1024^3))
          }'
