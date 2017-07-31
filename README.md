# CollectorProject
采集--每两分钟监测采集目录是否有新文件，有新文件则采集过来，对采集过的文件进行标记防止重复采集
  
  
    之前在电信研究院时，工作之一就维护一套程序，这套程序从Collect,ETL,Analysis每天200+亿条，8~9T的数据处理起来如丝顺滑，
    阅读这套程序过程中不管是代码规范还是设计理念都给予了我很大的帮助，所以我把其中的Collect部分抽取了出来单独做成一个采集项目，
    其实真正的精华在ETL,Analysis部分。
    
    
设计思路：
>程序以后台的方式常驻于采集机上，以轮询的方式监控采集目录，将采集目录添加到缓存队列和采集队列中，通过遍历采集队列中的目录文件来进行采集工作


 # 核心方法注释：
 
* getNewFiles：该方法主要是将所有需要采集的文件信息都保存在dateTimeToNewFilesMa【TreeMap】中

  * getModdifiedDirs(dirs):对需要copy的目录进行校验并缓存；（检验工作主要是判断该上当是否存在）
缓存规则：cacheLastModified==null || lastModified > cacheLastModified

  * isCopied方法:判断文件是否已经入过库;判断规则：读取target/_COPIED_FILES/yyyyMMhh/yyyyMMhh.txt文件，yyyyMMhh.txt的内容格式为：【文件绝对路径，时间戳，文件大小】，如果yyyyMMhh.txt文件不存在则说明该文件还并未入库，如果yyyyMMhh.txt文件存在，就比较入库文件的绝对路径是否被包含在yyyyMMhh.txt文件内容中
  
  
* copyFileSerially(dateTimeToNewFilesMap)：该方法主要是根据dateTimeToNewFilesMap中的文件信息来进行采集操作；
  1. 遍历dateTimeToNewFilesMap中的文件，依次调用collFile.copy方法进行采集
  2. 将copied的文件添加到copiedFiles队列中
  3. 遍历copiedFiles队列，将文件名定入到target/NEW_FILES/yyyyMMhh_UUID.txt(这个文件是为后面的ETL和Analysis做准备的)

* copyFilesParallel(dateTimeToNewFilesMap):与copyFileSerially的区别是该方法是并行采集文件


*  updateCopiedDataFiles(dateTime, copiedFiles)：该方法主要是将采集过的文件信息写入到target/_COPIED_FILES/yyyyMMhh/yyyyMMhh.txt中
