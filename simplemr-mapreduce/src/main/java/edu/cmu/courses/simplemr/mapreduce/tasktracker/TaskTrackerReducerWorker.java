package edu.cmu.courses.simplemr.mapreduce.tasktracker;

import edu.cmu.courses.simplemr.Constants;
import edu.cmu.courses.simplemr.Utils;
import edu.cmu.courses.simplemr.dfs.DFSClient;
import edu.cmu.courses.simplemr.mapreduce.MapReduce;
import edu.cmu.courses.simplemr.mapreduce.OutputCollector;
import edu.cmu.courses.simplemr.mapreduce.Pair;
import edu.cmu.courses.simplemr.mapreduce.task.MapperTask;
import edu.cmu.courses.simplemr.mapreduce.task.ReducerTask;
import edu.cmu.courses.simplemr.mapreduce.task.Task;
import netreducer.AsyncHttpRequest;
import netreducer.NRUtils;

import org.apache.commons.io.IOUtils;
import org.apache.http.Header;
import org.apache.http.HttpResponse;
import org.apache.http.message.BasicHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The task worker to do the reduce work. The reducer keep
 * track its partitions of files from every mapper. If all
 * the files are being collected, the mapper will merge the
 * files outside memory, and map it to the output directory
 * in DFS.
 *
 * @author Jian Fang(jianf)
 * @author Fangyu Gao(fangyug)
 */

public class TaskTrackerReducerWorker extends TaskTrackerWorker {

    public static final String MAPPER_RESULTS_DIR = "mappers";
    private static Logger LOG = LoggerFactory.getLogger(TaskTrackerReducerWorker.class);

    private PriorityQueue<MapperTask> mapperTasks;
    private ConcurrentHashMap<Integer, String> mapperFiles;
    private ConcurrentHashMap<Integer, Integer> mapperLocks;
    private Boolean finished;
    
    public TaskTrackerReducerWorker(Task task, TaskTracker taskTracker) {
        super(task, taskTracker);
        this.mapperTasks = new PriorityQueue<MapperTask>();
        this.mapperFiles = new ConcurrentHashMap<Integer, String>();
        this.mapperLocks = new ConcurrentHashMap<Integer, Integer>();
        this.finished = false;
    }

    public void createFolders(){
        task.createTaskFolder();
        String mapperResultsPath = task.getTaskFolderName() + Constants.FILE_SEPARATOR + MAPPER_RESULTS_DIR;
        File mapperResultsFolder = new File(getAbsolutePath(mapperResultsPath));
        if(!mapperResultsFolder.exists()){
            mapperResultsFolder.mkdirs();
        }
    }

    public void addMapperTask(MapperTask mapperTask){
        synchronized (mapperTasks){
            mapperTasks.add(mapperTask);
        }
    }

    public void updateReducerTask(ReducerTask task){
        this.task.setAttemptCount(task.getAttemptCount());
    }

    
    public void run() {
    	
        if(mapperFiles.size() == ((ReducerTask)task).getMapperAmount()){
            return;
        }
        MapperTask mapperTask = getNextMapperTask();
        if(mapperTask == null){
            return;
        }
        try {
        	
        	LOG.info("REDUCER ID:"+ ((ReducerTask)task).getTaskId());
        	if (task.isNetreducer()){
        		if(!copyMapperResultWithNR(mapperTask, ((ReducerTask)task).getTaskId())){
        			
        			taskTracker.reducerFailedOnMapper(((ReducerTask)task), mapperTask);
                    LOG.error("Reducer "+task.getTaskId()+" failed while getting the output of mapper: "+mapperTask.getTaskId());
                    return;
        		}
        	} else
        		copyMapperResult(mapperTask);
        	
        } catch (Exception e) {
            taskTracker.reducerFailedOnMapper(((ReducerTask)task), mapperTask);
            LOG.error("Reducer failed: ", e);
            return;
        }

        synchronized (finished){
            try{
                if(mapperFiles.size() == ((ReducerTask)task).getMapperAmount() && (!finished)){
                	long start = System.currentTimeMillis();
                    List<String> files = new ArrayList<String>(mapperFiles.values());
                    String unreducedFile = getAbsolutePath(getReducerResultFilePath("unreduced"));
                    String reducedFile = getAbsolutePath(getReducerResultFilePath(null));

                    if (task.isNetreducer())
                    	Utils.mergeFilesAndSort(files, unreducedFile);
                    else 
                    	Utils.mergeSortedFiles(files,unreducedFile);

                    OutputCollector collector = new OutputCollector();
                    MapReduce mr = newMRInstance();
                    long start2 = System.currentTimeMillis();
                    reduce(unreducedFile, mr, collector);
                    LOG.info("## REDUCERTIME #"+((ReducerTask) task).getTaskId()+":"+(System.currentTimeMillis()-start));
                    
                    saveResultToLocal(reducedFile, collector);
                    saveResultToDFS(reducedFile);

                    taskTracker.reducerSucceed(((ReducerTask)task));
                    finished = true;
                    //ibrahim
                    LOG.info("Job #"+((ReducerTask)task).getJobId()+": Reducer finished after (ms): "+(System.currentTimeMillis()- ((ReducerTask)task).getJobStartTime())+", startTime = "+((ReducerTask)task).getJobStartTime()
                    		+",task id = "+((ReducerTask)task).getTaskId());
                    LOG.info("Job #"+((ReducerTask)task).getJobId()+": task id = "+((ReducerTask)task).getTaskId()+", #reducers = "+((ReducerTask)task).getReducerAmount());
                    LOG.info("Reducer #"+((ReducerTask) task).getTaskId()+": run took (ms): "+(System.currentTimeMillis()-start));
                }
            } catch (Exception e){
                taskTracker.reducerFailed(((ReducerTask)task));
                LOG.error("Reducer failed: ", e);
            }
        }
    }

    private MapperTask getNextMapperTask(){
        MapperTask mapperTask = null;
        synchronized (mapperTasks){
            mapperTask = mapperTasks.poll();
        }
        return mapperTask;
    }

    private String getAbsolutePath(String fileName){
        return task.getOutputDir() + Constants.FILE_SEPARATOR + fileName;
    }

    private Integer getMapperLock(MapperTask mapperTask){
        Integer lock = mapperLocks.putIfAbsent(mapperTask.getTaskId(), mapperTask.getTaskId());
        return lock == null ? mapperTask.getTaskId() : lock;
    }

    private String getMapperResultFilePath(MapperTask mapperTask){
        return task.getTaskFolderName() + Constants.FILE_SEPARATOR +
               MAPPER_RESULTS_DIR + Constants.FILE_SEPARATOR + "mapper_" + mapperTask.getTaskId() + "_" +
               MapperTask.PARTITION_FILE_PREFIX +
               ((ReducerTask)task).getPartitionIndex();
    }

    private String getMapperResultURI(MapperTask mapperTask){
        return mapperTask.getTaskFolderName().replaceAll(Constants.FILE_SEPARATOR, "/") + "/" +
               MapperTask.PARTITION_FILE_PREFIX + ((ReducerTask)task).getPartitionIndex();
    }

    private String getReducerResultFilePath(String suffix){
        return task.getTaskFolderName() + Constants.FILE_SEPARATOR + ((ReducerTask)task).getOutputFile() +
               "_" + ((ReducerTask)task).getPartitionIndex() + (suffix == null ? "" : "_" + suffix);
    }

    private String getReducerResultFileName(){
        return ((ReducerTask)task).getOutputFile() + "_" + ((ReducerTask)task).getPartitionIndex();
    }

    private void copyMapperResult(MapperTask mapperTask) throws IOException {
        synchronized (getMapperLock(mapperTask)){
            if(mapperFiles.contains(mapperTask.getTaskId())){
                return;
            }
            InputStream in = Utils.getRemoteFile(mapperTask.getFileServerHost(),
                                                 mapperTask.getFileServerPort(),
                                                 getMapperResultURI(mapperTask));
            String outputFile = getAbsolutePath(getMapperResultFilePath(mapperTask));
            FileOutputStream out = new FileOutputStream(outputFile);
            IOUtils.copy(in, out);
            in.close();
            out.close();
            mapperFiles.put(mapperTask.getTaskId(), outputFile);
        }
    }
    
    private boolean copyMapperResultWithNR(MapperTask mapperTask, int treeId) throws Exception {
        synchronized (getMapperLock(mapperTask)){
        	
            if(mapperFiles.contains(mapperTask.getTaskId())){
            	LOG.warn("Output of Mapper "+mapperTask.getTaskId()+" already received.");
                return true;
            }
            
            String host = mapperTask.getFileServerHost();
            int port = mapperTask.getFileServerPort();
            String filePath = getMapperResultURI(mapperTask);
            String outputFile = getAbsolutePath(getMapperResultFilePath(mapperTask));
            
            int portNR = 10000+mapperTask.getTaskId()+(100*treeId);
            
            AsyncHttpRequest ahr = new AsyncHttpRequest();
            try {
                
            	Header[] headers = new Header[2];
            	headers[0] = new BasicHeader("NRport", String.valueOf(portNR));
            	headers[1] = new BasicHeader("NRtreeId", String.valueOf(treeId));
            	
            	LOG.info("Request: "+"http://" + host + ":" + port + "/" + filePath);
                // Execute request
            	ahr.asyncHttpGet("http://" + host + ":" + port + "/" + filePath,headers);
                
                if (!NRUtils.receiveFile(outputFile, treeId, "0.0.0.0", portNR))
                	return false;
                
                mapperFiles.put(mapperTask.getTaskId(), outputFile);
                
                HttpResponse response = ahr.getResponse();
                
                LOG.info("Response: "+ahr.getRequest().getRequestLine() + "->" + response.getStatusLine());

                ahr.close();
                
                return true;
                
            } catch (Exception e) {
                LOG.error("HTTP client failed: ", e);
                
                throw e;
            } 
        }
    }

    private void reduce(String inputFile, MapReduce mr, OutputCollector collector)
            throws IOException {
        BufferedReader reader = new BufferedReader(new FileReader(inputFile));
        String line = null;
        String key = null;
        String prevKey = null;
        List<String> values = new ArrayList<String>();
        while(true){
            line = reader.readLine();
            if(line == null){
                if(key != null){
                    mr.reduce(key, values.iterator(), collector);
                }
                break;
            }
            Pair<String, String> entry = Utils.splitLine(line);
            key = entry.getKey();
            if(prevKey != null && !key.equals(prevKey)){
                mr.reduce(prevKey, values.iterator(), collector);
                values.clear();
            }
            values.add(entry.getValue());
            prevKey = key;
        }
        reader.close();
    }

    private void saveResultToLocal(String localFileName, OutputCollector collector)
            throws IOException {
        FileWriter writer = new FileWriter(localFileName);
        Iterator<Pair<String, String>> iterator = collector.getIterator();
        while(iterator.hasNext()){
            Pair<String, String> entry = iterator.next();
            writer.write(entry.getKey() + Constants.MAPREDUCE_DELIMITER + entry.getValue() + "\n");
        }
        writer.flush();
        writer.close();
    }

    private void saveResultToDFS(String localFile)
            throws Exception {
        DFSClient dfsClient = new DFSClient(taskTracker.getDfsMasterRegistryHost(),
                                            taskTracker.getDfsMasterRegistryPort());
        dfsClient.connect();
        dfsClient.writeText(localFile, ((ReducerTask)task).getReplicas(), ((ReducerTask)task).getLineCount());
    }
}
