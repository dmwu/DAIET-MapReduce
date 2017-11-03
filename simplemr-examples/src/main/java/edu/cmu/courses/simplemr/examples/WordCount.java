package edu.cmu.courses.simplemr.examples;

import edu.cmu.courses.simplemr.mapreduce.AbstractMapReduce;
import edu.cmu.courses.simplemr.mapreduce.OutputCollector;
import edu.cmu.courses.simplemr.mapreduce.jobtracker.JobInfo;
import edu.cmu.courses.simplemr.mapreduce.jobtracker.JobTrackerScheduler;
import edu.cmu.courses.simplemr.mapreduce.task.MapperTask;

import java.net.InetAddress;
import java.util.Iterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The word count example.
 * Count the occurrence time of words.
 * In put a line of file.
 * Output in the format:
 * "word number"
 *
 * @author Jian Fang(jianf)
 * @author Fangyu Gao(fangyug)
 */

public class WordCount extends AbstractMapReduce {
    private static Logger LOG = LoggerFactory.getLogger(WordCount.class);
    public void map(String key, String value, OutputCollector collector) {
        
        //ibrahim
//        String hostName = "";
//        try{
//        	hostName = InetAddress.getLocalHost().getHostName();
//        }catch(Exception ex){}
    	
//    	long start = System.currentTimeMillis();
    	String[] words = value.split("\\s+");
        for(String word : words){
            collector.collect(word, "1");
        }
//        LOG.info("map took "+(System.currentTimeMillis()-start));
    }

    public void reduce(String key, Iterator<String> values, OutputCollector collector) {
        int sum = 0;
        while(values.hasNext()){
        	sum += Integer.parseInt(values.next());
//            values.next();
        }
        collector.collect(key, String.valueOf(sum));
    }

    public static void main(String[] args) {
    	
    	//ibrahim
    	WordCount wc = new WordCount();
    	wc.run(args);
    	JobInfo.runTime = 0;
    	JobInfo.startTime = System.currentTimeMillis(); 
        LOG.info("WordCount.main: "+"JobName: "+wc.jobName+", outputFile: "+wc.getJobConfig().getOutputFile()+", combiner = "+wc.getJobConfig().getCombiner()+", startTime = "+JobInfo.startTime);
    	System.out.println("JobName: "+wc.jobName+", outputFile: "+wc.getJobConfig().getOutputFile()+", combiner = "+wc.getJobConfig().getCombiner()); 
    	
        //new WordCount().run(args);
    }
}
