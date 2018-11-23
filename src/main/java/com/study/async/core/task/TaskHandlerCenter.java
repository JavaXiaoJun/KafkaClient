package com.study.async.core.task;

import com.study.async.core.AbstractTask;
import com.study.async.entity.TaskSource;
import com.study.async.entity.handler.TaskHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.*;

/**
 * Created by lf52 on 2018/11/22.
 */
public class TaskHandlerCenter<T> {

    protected static Logger logger = LoggerFactory.getLogger(TaskHandlerCenter.class);

    private volatile Status status = Status.INIT;

    private TaskSource tasksource;
    private TaskHandler handler;
    private int taskThreadNum =  Runtime.getRuntime().availableProcessors();
    private int threadNum = 0;
    private int minThreadNum = 0;
    private int maxThreadNum = 0;

    private ExecutorService taskThreadPool;

    private ExecutorService sharedAsyncThreadPool;

    private boolean isAsyncThreadModel = false;
    private boolean isSharedAsyncThreadPool = false;

    private List<AbstractTask> tasks;

    /**
     * task center status
     */
    enum Status {
        INIT, RUNNING, STOPPING, STOPPED;
    }



    /**
     *  异步线程模式
     * @param tasksource
     * @param handler
     * @param isSharedAsyncThreadPool
     * @param threadNum
     * @param minThreadNum
     * @param maxThreadNum
     */
    public TaskHandlerCenter(TaskSource tasksource,TaskHandler handler, boolean isSharedAsyncThreadPool,int threadNum, int minThreadNum, int maxThreadNum) {

        this.tasksource = tasksource;
        this.handler = handler;
        this.threadNum = threadNum;
        this.minThreadNum = minThreadNum;
        this.maxThreadNum = maxThreadNum;
        this.isAsyncThreadModel = true;
        this.isSharedAsyncThreadPool = isSharedAsyncThreadPool;
        init();

    }

    public TaskHandlerCenter(TaskSource tasksource,TaskHandler handler,int taskThreadNum) {

        this.tasksource = tasksource;
        this.handler = handler;
        this.taskThreadNum = taskThreadNum;
        init();

    }

    /**
     * task center 启动入口
     */
    public void startup() {

        if (status != Status.INIT) {
            throw new IllegalStateException("The task center has been started.");
        }

        List<T> ts = tasksource.getT();
        status = Status.RUNNING;

        logger.info("Tasks num : " + ts.size());

        for (int i = 0; i < ts.size() ; i++) {
            AbstractTask task = (isAsyncThreadModel?new ConcurrentTask(ts.get(i),handler):new SequentialTask<>(ts.get(i),handler));
            tasks.add(task);
            taskThreadPool.submit(task);
        }


    }


    /**
     * init task center
     */
    private void init() {

        if (isAsyncThreadModel == true && threadNum <= 0
                && (minThreadNum <= 0 || maxThreadNum <= 0)) {
            throw new IllegalArgumentException(
                    "Either fixedThreadNum or minThreadNum/maxThreadNum is greater than 0.");
        }

        if (isAsyncThreadModel == true && minThreadNum > maxThreadNum) {
            throw new IllegalArgumentException(
                    "The minThreadNum should be less than maxThreadNum.");
        }

        if (isAsyncThreadModel == true && isSharedAsyncThreadPool) {
            sharedAsyncThreadPool = initThreadPool();
        }

        tasks = new LinkedList<>();

        initGracefullyShutdown();

        taskThreadPool = Executors.newFixedThreadPool(taskThreadNum);

    }

    /**
     * 增加一个钩子事件，在jvm退出时调用
     */
    protected void initGracefullyShutdown() {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                shutdownGracefully();
            }
        });
    }

    /**
     * 实现优雅的关机（释放所有使用的资源），邮件预警等
     */
    public void shutdownGracefully() {

        status = Status.STOPPING;

        shutdownThreadPool(taskThreadPool);

        if(isAsyncThreadModel){
            if (isSharedAsyncThreadPool)
                shutdownThreadPool(sharedAsyncThreadPool);
            else
                tasks.forEach(task->{
                    task.shutdown();
                });
        }

        status = Status.STOPPED;

        //todo
        System.out.println("jvm exit");
    }

    //inner class==============================================================================================================================================

    /**
     * Created by lf52 on 2018/11/22.
     *
     * 异步模式任务：包括共享任务线程池（适用于快速返回的型的任务）和独立使用线程池（适用于耗时的操作）
     */
     class ConcurrentTask<T> extends AbstractTask<T> {

        private ExecutorService asyncThreadPool;

        private TaskHandler handler;

        private ConcurrentTask(T t,TaskHandler handler) {
            super(t);
            this.handler = handler;
            if (isSharedAsyncThreadPool)
                asyncThreadPool = sharedAsyncThreadPool;
            else {
                asyncThreadPool = initThreadPool();
            }
        }

        @Override
        protected void handleMessage(T t) {
            handler.executeAsync(t,asyncThreadPool);
        }

        @Override
        public void shutdown() {
            if(asyncThreadPool != null && !asyncThreadPool.isTerminated()){
                shutdownThreadPool(asyncThreadPool);
            }
        }

    }

    /**
     * Created by lf52 on 2018/11/22.
     *
     * 同步任务模式
     */
    public class SequentialTask<T> extends AbstractTask<T> {

        private TaskHandler handler;

        public SequentialTask(T tasksource,TaskHandler handler) {
            super(tasksource);
            this.handler = handler;
        }

        @Override
        protected void handleMessage(T tasksource) {
             handler.executeSync(tasksource);
        }

        @Override
        public void shutdown() {
            //do nothing
        }

    }

    /**
     * 初始化线程池
     * @return
     */
    private ExecutorService initThreadPool() {
        ExecutorService threadPool = null;
        if (threadNum > 0)
            threadPool = Executors.newFixedThreadPool(threadNum);

        else
            threadPool = new ThreadPoolExecutor(minThreadNum, maxThreadNum,
                    60L, TimeUnit.SECONDS, new SynchronousQueue<Runnable>());
        return threadPool;
    }

    /**
     * 关闭线程池及其执行的线程
     * @param threadPool
     */
    protected void shutdownThreadPool(ExecutorService threadPool) {
        logger.info("Start to shutdown the thead pool: {}", threadPool);
        threadPool.shutdown();
        try {
            if (!threadPool.awaitTermination(60, TimeUnit.SECONDS)) {
                threadPool.shutdownNow();
                logger.warn("Interrupt the worker, which may cause some task inconsistent. Please check the biz logs.");

                if (!threadPool.awaitTermination(60, TimeUnit.SECONDS))
                    logger.error("Thread pool can't be shutdown even with interrupting worker threads, which may cause some task inconsistent. Please check the biz logs.");
            }
        } catch (InterruptedException ie) {
            threadPool.shutdownNow();
            logger.error("The current server thread is interrupted when it is trying to stop the worker threads. This may leave an inconcistent state. Please check the biz logs.");

            Thread.currentThread().interrupt();
        }
        logger.info("Finally shutdown the thead pool: {}", threadPool);
    }


}
