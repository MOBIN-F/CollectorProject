package com.mobin.collector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * Created by Mobin on 2017/5/6.
 * 实现ThreadFactory定制守护线程，之所以使用守护线程是因为采集程序需要一个长驻的不断轮询的线程
 */
public class ExecutorUtils {

    private static final Logger log = LoggerFactory.getLogger(ExecutorUtils.class);

    public static volatileExecutor createVolatileExecutor(String name) {
        return createVolatileExecutor(name, -1);
    }

    public static volatileExecutor createVolatileExecutor(String name, int maximumPoolSize){
        if (maximumPoolSize <= 0) {
            maximumPoolSize = Runtime.getRuntime().availableProcessors();
        }
        return new volatileExecutor(1, maximumPoolSize, 60L, TimeUnit.SECONDS, new SynchronousQueue<Runnable>(),
                new DeamonThreadFactory(name));
    }

    public static class volatileExecutor implements AutoCloseable{
        private final ArrayList<Future<?>> futures = new ArrayList<>();
        private final ArrayList<Object> tasks = new ArrayList<>();
        private  final ThreadPoolExecutor threadPoolExecutor;

        public volatileExecutor(int corePoolSize,   //核心线程，池中所保存的线程数
                                int maximumPoolSize,         //最大线程数，可创建的最大线程数
                                long keepAileTime,              //如果线程数大于corePoolSize,则这些多余的线程空闲时间超过该参数将被终止
                                TimeUnit unit,                      //keepAileTime的时间单位
                                BlockingQueue<Runnable> workQueue,   //保存任务的阻塞队列
                                ThreadFactory threadFactory){
            threadPoolExecutor = new ThreadPoolExecutor(corePoolSize, maximumPoolSize, keepAileTime, unit, workQueue, threadFactory);
            threadPoolExecutor.setRejectedExecutionHandler(blockingExecutorHandler);
            threadPoolExecutor.allowsCoreThreadTimeOut();

        }

        @Override
        public void close() throws Exception {
              if (!futures.isEmpty()) {
                  await();
              }
            threadPoolExecutor.shutdown();
        }

        //传入task，要把task的类型来执行任务
        public void submitTasks(List<?> tasks) {
            for (Object task : tasks) {
                if (task instanceof  Runnable) {
                    submitTask((Runnable) task);
                }else if (task instanceof  Callable) {
                    submitTask((Callable<?>) task);
                }else {
                    log.warn("Invalid task: " + task);
                }
            }
        }

        public void submitTask(Runnable task){
            try {
                futures.add(threadPoolExecutor.submit(task));   // 方便获取任务执行结果
                tasks.add(task);
            }catch (Exception e){
                log.info("Failed to submit tabks " + task + "," + e);
            }
        }

        public void submitTask(Callable<?> task) {
            try {

                futures.add(threadPoolExecutor.submit(task));
                tasks.add(task);
            }catch (Exception e) {
                log.error("Failed to submit task: " + task + "," + e);
            }
        }

        public void await() {
            for (int i = 0, size = futures.size(); i < size; i  ++) {
                try {
                    futures.get(i).get();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            futures.clear();
            tasks.clear();
        }
    }

    //定义被拒绝处理任务的策略
    public static final RejectedExecutionHandler blockingExecutorHandler = new RejectedExecutionHandler() {
        @Override
        public void rejectedExecution(Runnable task, ThreadPoolExecutor executor) {
            BlockingQueue<Runnable> queue = executor.getQueue();
            while (true) {
                if (executor.isShutdown()) {
                    throw  new RejectedExecutionException("TheadPoolExecutor has shut down!");
                }
                try {
                    if (queue.offer(task, 1000, TimeUnit.MILLISECONDS)){
                        break;
                    }
                } catch (InterruptedException e) {
                    throw new AssertionError(e);
                }
            }

        }
    };

    public static class DeamonThreadFactory implements  ThreadFactory {
        private final String id;
        private final int priority;
        private final AtomicInteger n = new AtomicInteger(1);

        public DeamonThreadFactory(){
            this.id = "mobin-thread";
            this.priority = Thread.NORM_PRIORITY;
        }

        public DeamonThreadFactory(String id) {
            this(id, Thread.NORM_PRIORITY);
        }

        public DeamonThreadFactory(String id, int priority){
            this.id = "mobin" + id + "-thread";
            this.priority = priority;
        }

        @Override
        public Thread newThread(Runnable runnable) {
            String name = id + "-" + n.getAndIncrement();
            Thread thread = new Thread(runnable,name);
            thread.setPriority(priority);
            thread.setDaemon(true);    //守护线程
            return thread;
        }
    }
}
