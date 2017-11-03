package edu.cmu.courses.simplemr.mapreduce.tasktracker;

import edu.cmu.courses.simplemr.Constants;
import edu.cmu.courses.simplemr.Utils;
import edu.cmu.courses.simplemr.mapreduce.MapReduce;
import edu.cmu.courses.simplemr.mapreduce.OutputCollector;
import edu.cmu.courses.simplemr.mapreduce.Pair;
import edu.cmu.courses.simplemr.mapreduce.io.DFSFileReader;
import edu.cmu.courses.simplemr.mapreduce.task.MapperTask;
import edu.cmu.courses.simplemr.mapreduce.task.Task;
import netreducer.NRUtils;

import java.io.BufferedOutputStream;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The task worker to do the map work. It run the user map function
 * and save the result to local. Use TreeMap to automatically sort
 * on mapper side, and can handle duplicate keys and values.
 *
 * @author Jian Fang(jianf)
 * @author Fangyu Gao(fangyug)
 */

public class TaskTrackerMapperWorker extends TaskTrackerWorker{
    private DFSFileReader reader;
    private static Logger LOG = LoggerFactory.getLogger(TaskTrackerMapperWorker.class);

    public TaskTrackerMapperWorker(Task task, TaskTracker taskTracker) {
        super(task, taskTracker);
        reader = new DFSFileReader(taskTracker.getDfsMasterRegistryHost(),
                                   taskTracker.getDfsMasterRegistryPort(),
                                   ((MapperTask)task).getInputFileBlock());
    }
    
    public void run() {
        try {
        	long start = System.currentTimeMillis();
            OutputCollector collector = collect();
            
            if (task.isNetreducer())
            	saveToLocalNR(collector);
            else
            	saveToLocal(collector);
            
            taskTracker.mapperSucceed((MapperTask) task);
            LOG.info("Mapper #"+((MapperTask) task).getTaskId()+": run took (ms): "+(System.currentTimeMillis()-start));
        } catch (Exception e) {
            taskTracker.mapperFailed((MapperTask) task);
            LOG.error("Mapper #"+((MapperTask) task).getTaskId()+" "+e.getMessage(),e);
        }
    }
  
    private OutputCollector collect()
            throws Exception {
        String line = null;
        MapReduce mr = newMRInstance();
        OutputCollector collector = new OutputCollector();
        reader.open();
        long start=System.currentTimeMillis();
        int numLines = 0;
        long timeInSplit = 0;
        long timeInMap = 0;
        long timeInReading = 0;
        long tmp =0;
        LOG.info("Mapper #"+((MapperTask) task).getTaskId()+": starting the loop");
//        while((line = reader.readLine()) != null){
        line = reader.readLine();
        while(line != null){	
        	tmp = System.currentTimeMillis();
            Pair<String, String> entry = Utils.splitLine(line);
            //LOG.info("Mapper #"+((MapperTask) task).getTaskId()+": line#"+numLines+" - splitting took "+(System.currentTimeMillis()-tmp));
            timeInSplit+=(System.currentTimeMillis()-tmp);
            tmp = System.currentTimeMillis();
            mr.map(entry.getKey(), line, collector);
            //LOG.info("Mapper #"+((MapperTask) task).getTaskId()+":line#"+numLines+" - map took "+(System.currentTimeMillis()-tmp)	);
            timeInMap+=(System.currentTimeMillis()-tmp);
            numLines++;
            tmp = System.currentTimeMillis();
            line = reader.readLine();
            timeInReading+=(System.currentTimeMillis()-tmp);
        }
//        tmp = System.currentTimeMillis();
        reader.close();
        LOG.info("Mapper #"+((MapperTask) task).getTaskId()+": done with the loop");
//        long timeReaderClose =(System.currentTimeMillis()-tmp);
        LOG.info("Mapper #"+((MapperTask) task).getTaskId()+": processing "+numLines+" lines took "+(System.currentTimeMillis()-start)
        		+", time in split = "+timeInSplit+", time in map = "+timeInMap+", timeInReading = "+timeInReading);
        return collector;
    }
    
    private void saveToLocal(OutputCollector collector)
            throws IOException {
    	long start = System.currentTimeMillis();
        String folderName = ((MapperTask)task).getOutputDir() + Constants.FILE_SEPARATOR + task.getTaskFolderName();
        File[] outputFiles = new File[((MapperTask)task).getReducerAmount()];
        
        //ibrahim
        LOG.info("TaskTrackerMapperWorker.saveToLocal.folderName: " + folderName);
        LOG.info("TaskTrackerMapperWorker.saveToLocal.outputFiles.length: " + outputFiles.length);
        
        for(int i = 0; i < outputFiles.length; i++){
            outputFiles[i] = new File(folderName + Constants.FILE_SEPARATOR + MapperTask.PARTITION_FILE_PREFIX + i);
            outputFiles[i].createNewFile();
        }
        LOG.info("WordCount.useCombiner: " + ((MapperTask)task).getCombiner());
        TreeMap<String, List<String>> recordMap = collector.getMap();
        int mapSize = recordMap.size();
        int rangeCount = Math.min(recordMap.size(), ((MapperTask)task).getReducerAmount());
        int rangeSize = mapSize / rangeCount;
        for(int i = 0; i < rangeCount; i++){
            int startKeyIndex = i * rangeSize;
            int endKeyIndex = (i + 1) * rangeSize;
            if(i == rangeCount - 1){
                endKeyIndex = mapSize;
            }
            BufferedWriter writer = new BufferedWriter(new FileWriter(outputFiles[i]));
            for(int j = startKeyIndex; j < endKeyIndex; j++){
                Map.Entry<String, List<String>> entry = recordMap.pollFirstEntry();
                List<String> values = entry.getValue();
                //ibrahim
                if(((MapperTask)task).getCombiner()==1) //sum
                {
	                //ibrahim add below
//                	int count = 0;
//                	for(String value : values){
//                		count++;
//                	}
                	writer.write(entry.getKey() + Constants.MAPREDUCE_DELIMITER + values.size());
	                writer.newLine();
                }
                else
                	if(((MapperTask)task).getCombiner()==2) //max
                    {
    	                int max = Integer.MIN_VALUE;
    	                for(String value : values){
                    		int v = Integer.parseInt(value);
                    		if(v>max)
                    			max = v;
                    	}
    	                
                    	writer.write(entry.getKey() + Constants.MAPREDUCE_DELIMITER + max);
    	                writer.newLine();
                    }
                	else
                    	if(((MapperTask)task).getCombiner()==3) //min
                        {
        	                int min = Integer.MAX_VALUE;
        	                for(String value : values){
                        		int v = Integer.parseInt(value);
                        		if(v<min)
                        			min = v;
                        	}
        	                
                        	writer.write(entry.getKey() + Constants.MAPREDUCE_DELIMITER + min);
        	                writer.newLine();
                        }
		                else 
		                	if(((MapperTask)task).getCombiner()==0){
			                	for(String value : values){
			                		writer.write(entry.getKey() + Constants.MAPREDUCE_DELIMITER + value);
			                		writer.newLine();
			                	}
		                	}
//                if(((MapperTask)task).)
                //ibrahim: comment below
                
            }
            writer.flush();
            writer.close();
        }
        LOG.info("Mapper #"+((MapperTask) task).getTaskId()+": saveToLocal took (ms): "+(System.currentTimeMillis()-start));
    }
    
    private void saveToLocalNR(OutputCollector collector)
            throws IOException {
    	
    	long start = System.currentTimeMillis();
        String folderName = ((MapperTask)task).getOutputDir() + Constants.FILE_SEPARATOR + task.getTaskFolderName();
        String[] outputFiles = new String[((MapperTask)task).getReducerAmount()];
        OutputStream outputStream = null;
        
        //** ibrahim
        LOG.info("TaskTrackerMapperWorker.saveToLocal.folderName: " + folderName);
        LOG.info("TaskTrackerMapperWorker.saveToLocal.outputFiles.length: " + outputFiles.length);
        //**
        
        for(int i = 0; i < outputFiles.length; i++){
            outputFiles[i] = new String(folderName + Constants.FILE_SEPARATOR + MapperTask.PARTITION_FILE_PREFIX + i);
            
            outputStream = new BufferedOutputStream(new FileOutputStream(new File(outputFiles[i])));
    		
    		// File preamble
    		outputStream.write(ByteBuffer.allocate(4).putInt(0).array());
    		outputStream.close();
        }
        
        LOG.info("WordCount.useCombiner: " + ((MapperTask)task).getCombiner());
        
        Map<String, List<Integer>> recordMap = collector.getMapOutput(((MapperTask)task).getCombiner());
        
        int mapSize = recordMap.size();
        int rangeCount = Math.min(recordMap.size(), ((MapperTask)task).getReducerAmount());
        int rangeSize = mapSize / rangeCount;
        
        String[] keys = recordMap.keySet().toArray(new String[0]);
    	
        Comparator<Pair<String, Integer>> comparator = new Comparator<Pair<String, Integer>>() {
            public int compare(Pair<String, Integer> o1, Pair<String, Integer> o2) {
                return o1.getKey().compareTo(o2.getKey());
            }
        };

        PriorityQueue<Pair<String,Integer>> partition = new PriorityQueue<Pair<String, Integer>>(11,comparator);
        
        for(int i = 0; i < rangeCount; i++){
        	
            int startKeyIndex = i * rangeSize;
            int endKeyIndex = (i + 1) * rangeSize;
            
            if(i == rangeCount - 1){
                endKeyIndex = mapSize;
            }
            
            for(int j = startKeyIndex; j < endKeyIndex; j++){
            	
            	for(Integer value : recordMap.get(keys[j]))
            		partition.add(new Pair<String,Integer>(keys[j], value));
            }
            
            NRUtils.writeFile(partition, outputFiles[i]);
            partition.clear();
        }
        
        LOG.info("Mapper #"+((MapperTask) task).getTaskId()+": saveToLocal took (ms): "+(System.currentTimeMillis()-start));
    }
}
