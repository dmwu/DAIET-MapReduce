package edu.cmu.courses.simplemr.mapreduce.io;

import edu.cmu.courses.simplemr.dfs.DFSChunk;
import edu.cmu.courses.simplemr.dfs.DFSClient;
import edu.cmu.courses.simplemr.dfs.DFSFile;
import edu.cmu.courses.simplemr.mapreduce.tasktracker.TaskTrackerMapperWorker;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Read from distributed file system.
 *
 * @author Jian Fang(jianf)
 * @author Fangyu Gao(fangyug)
 */

public class DFSFileReader extends FileReader {

    private DFSClient dfsClient;
    private DFSFile dfsFile;
    private long currentOffset;
    private StringBuffer chunkBuffer;
    private static Logger LOG = LoggerFactory.getLogger(DFSFileReader.class);
    private int readOffset = 0;
    public DFSFileReader(String registryHost, int registryPort, FileBlock fileBlock) {
        super(fileBlock);
        this.dfsClient = new DFSClient(registryHost, registryPort);
//        this.chunkBuffer = new StringBuffer();//ibrahim
        this.chunkBuffer = new StringBuffer();
        this.readOffset = 0;
        this.currentOffset = fileBlock.getOffset();
    }

    @Override
    public void open() throws Exception {
        dfsClient.connect();
        dfsFile = dfsClient.getFile(fileBlock.getFile());
        if(dfsFile == null){
            throw new IOException("can't get file metadata");
        }
    }

    @Override
    public void close() {

    }

    @Override
    public String readLine() throws Exception {
//    	long start = System.currentTimeMillis();
//    	long chunkReadTime  = 0;
//    	long lineReadTime  = 0;
//        StringBuffer lineBuffer = new StringBuffer();//ibrahim
    	StringBuffer lineBuffer = new StringBuffer();
    	
        while(true){
            if(fileBlock.getSize() >= 0 && currentOffset == fileBlock.getOffset() + fileBlock.getSize()){
                break;
            }
            //if(chunkBuffer.length() == 0)
            //if(chunkBuffer.length() == readOffset)
            if(chunkBuffer.length() == readOffset)
            {
            	long tmp = System.currentTimeMillis();
                DFSChunk chunk = getNextChunk();
                if(chunk == null){
                    break;
                }
                long chunkOffset = currentOffset - chunk.getOffset();
                int chunkSize = 0;
                if(fileBlock.getSize() < 0){
                    chunkSize = (int) (chunk.getSize() - chunkOffset);
                } else {
                    chunkSize = (int)(Math.min(fileBlock.getOffset() + fileBlock.getSize(),
                                               chunk.getOffset() + chunk.getSize()) - currentOffset);
                }
                byte[] data = dfsClient.readChunk(chunk, chunkOffset, chunkSize);
                if(data == null || data.length == 0){
                    break;
                }
                chunkBuffer.append(new String(data));
//                readOffset=0;
//                chunkReadTime+= (System.currentTimeMillis()-tmp);
            }
            //////////////////////////////////////////////////////
             
            //slow
            /*long tmp = System.currentTimeMillis();
            
            boolean hasNewLine = false;
            int i=0;
            String chunkBufferStr =chunkBuffer.toString(); 
            chunkBuffer = new StringBuffer();
            while(i<chunkBufferStr.length() && chunkBufferStr.charAt(i)!='\n'){
            	lineBuffer.append(chunkBufferStr.charAt(i));
            	i++;
            }
            if(i<chunkBufferStr.length() && chunkBufferStr.charAt(i)=='\n'){
            	i++;
            	hasNewLine=true;
            }
        	currentOffset+=i;
        	for(;i<chunkBufferStr.length();i++)
        		chunkBuffer.append(chunkBufferStr.charAt(i));
            lineReadTime+=(System.currentTimeMillis()-tmp);
            if(hasNewLine)
            	break;
            */
            
            /*
            long tmp = System.currentTimeMillis();
            boolean hasNewLine = false;
            int i=0;
            for(;i<chunkBuffer.length();i++){
            	if(chunkBuffer.charAt(i)=='\n')
            	{
            		hasNewLine=true;
            		break;
            	}
            }
            String str = chunkBuffer.toString();
            lineBuffer.append(str.substring(0,i));
            chunkBuffer = new StringBuilder(1000);
            if(!hasNewLine)
            {
            	chunkBuffer.append(str.substring(i, str.length()));
            	currentOffset+=i;
            }
            else{
            	chunkBuffer.append(str.substring(i+1, str.length()));
            	currentOffset+=i+1;
            }
            lineReadTime+=(System.currentTimeMillis()-tmp);
            if(hasNewLine)
            	break;
            */
            
            
            char ch = chunkBuffer.charAt(readOffset);
            readOffset++;
            //chunkBuffer.deleteCharAt(0);
            currentOffset++;
            lineBuffer.append(ch);
            if(ch == '\n'){
                break;
            }
            
            /*
            char ch = chunkBuffer.charAt(0);
            chunkBuffer.deleteCharAt(0);
            currentOffset++;
            lineBuffer.append(ch);
            if(ch == '\n'){
                break;
            }
            */
        }
        if(lineBuffer.length() == 0){
            return null;
        } else {
            if(lineBuffer.charAt(lineBuffer.length() - 1) == '\n'){
                lineBuffer.deleteCharAt(lineBuffer.length() - 1);
            }
//            LOG.info("Read line took: "+(System.currentTimeMillis()-start)+", line.length="+lineBuffer.length()
//            		+", chunkReadTime = "+chunkReadTime+", lineReadTime = "+lineReadTime);
            return lineBuffer.toString();
        }
        
    }

    private DFSChunk getNextChunk(){
        for(DFSChunk chunk : dfsFile.getChunks()){
            if(chunk.getOffset() <= currentOffset &&
               chunk.getOffset() + chunk.getSize() > currentOffset){
                return chunk;
            }
        }
        return null;
    }
}
