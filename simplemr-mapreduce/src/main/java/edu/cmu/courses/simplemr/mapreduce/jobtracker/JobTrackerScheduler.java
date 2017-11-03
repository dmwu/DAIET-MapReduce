package edu.cmu.courses.simplemr.mapreduce.jobtracker;

import edu.cmu.courses.simplemr.Constants;
import edu.cmu.courses.simplemr.mapreduce.task.MapperTask;
import edu.cmu.courses.simplemr.mapreduce.task.Task;
import edu.cmu.courses.simplemr.mapreduce.task.TaskStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * The job tracker scheduler start to dispatch the map tasks.
 *
 * @author Jian Fang(jianf)
 * @author Fangyu Gao(fangyug)
 */

public class JobTrackerScheduler implements Runnable{
    private static Logger LOG = LoggerFactory.getLogger(JobTrackerScheduler.class);

    private JobTracker jobTracker;
    private ExecutorService pool;

    public JobTrackerScheduler(JobTracker jobTracker, int poolSize){
        this.jobTracker = jobTracker;
        if(poolSize <= 0){
            this.pool = Executors.newFixedThreadPool(Constants.DEFAULT_SCHEDULED_THREAD_POOL_SIZE);
            LOG.info("ThreadPool is created: #def threads = "+Constants.DEFAULT_SCHEDULED_THREAD_POOL_SIZE);
        } else {
            
        	//ibrahim: changes to thread pool 
            this.pool = Executors.newFixedThreadPool(poolSize);
//        	this.pool = Executors.newCachedThreadPool();
            
            LOG.info("ThreadPool is created: #threads = "+poolSize);
        }
        
    }

    
    public void run() {
        while(true){
            try {
            	LOG.info("Trying to pull a MapperTask");
            	MapperTask task = jobTracker.takeMapperTask();
                LOG.info("Pulling a MapperTask is done, now changing status to pending");
                task.setStatus(TaskStatus.PENDING);
                JobTrackerDispatcher dispatcher = new JobTrackerDispatcher(jobTracker, task);
                LOG.info("Try to dispatch the MapperTask");
                pool.execute(dispatcher);
                LOG.info("MapperTask dispatching is done");
            } catch (InterruptedException e) {
                LOG.error("Job scheduler is interrupted!", e);
                System.exit(-1);
            }
        }
    }
}
