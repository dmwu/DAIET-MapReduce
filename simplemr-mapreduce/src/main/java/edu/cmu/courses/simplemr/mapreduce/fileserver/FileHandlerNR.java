package edu.cmu.courses.simplemr.mapreduce.fileserver;

import edu.cmu.courses.simplemr.Constants;
import edu.cmu.courses.simplemr.mapreduce.task.MapperTask;
import netreducer.NRUtils;

import org.apache.commons.fileupload.FileItem;
import org.apache.commons.fileupload.disk.DiskFileItemFactory;
import org.apache.commons.fileupload.servlet.ServletFileUpload;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.RateLimiter;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URLConnection;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * The handler of files sent from mappers to reducers.
 *
 * @author Amedeo Sapio
 */

public class FileHandlerNR extends HttpServlet {
    
	private static final long serialVersionUID = 1L;
	
	private static Logger LOG = LoggerFactory.getLogger(FileHandlerNR.class);
	private String baseDir;
    private DiskFileItemFactory factory;
    private ServletFileUpload uploadHandler;
    
    private static final int DEFAULT_BUFFER_SIZE = 1024 * 4;
    
    public FileHandlerNR(String baseDir){
        this.baseDir = baseDir;
        this.factory = new DiskFileItemFactory();
        this.uploadHandler = new ServletFileUpload(factory);
    }

    @Override
    public void doGet(HttpServletRequest request, HttpServletResponse response)
            throws IOException {
    	
        String filePath = baseDir + request.getRequestURI().replaceAll("/", Constants.FILE_SEPARATOR);
        File file = new File(filePath);
        String nrport = request.getHeader("NRport");
        String nrtreeId = request.getHeader("NRtreeId");
        
        if (file.getName().startsWith(MapperTask.PARTITION_FILE_PREFIX) && nrport!=null && nrtreeId!=null){
        	
        	// MapOutput request
        	
        	if(!file.exists()){
        	
	            response.setStatus(HttpServletResponse.SC_NOT_FOUND);
	            try {
					NRUtils.sendEND(request.getRemoteAddr(),Integer.parseInt(nrport),Integer.parseInt(nrtreeId));
				} catch (IOException | NumberFormatException | InterruptedException e) {
					LOG.error("Error while sending ERROR: ",e);
				}
	            return;
	        }
	        if(file.isDirectory()){
	        
	            response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
	            try {
					NRUtils.sendERROR(request.getRemoteAddr(),Integer.parseInt(nrport),Integer.parseInt(nrtreeId));
				} catch (IOException e) {
					LOG.error("Error while sending ERROR: ",e);
				}
	            return;
	        }
	        
        	try{
        	
        		NRUtils.sendFile(filePath, request.getRemoteAddr(),Integer.parseInt(nrport),Integer.parseInt(nrtreeId));
        	} catch (Exception e){
        		LOG.error("NR file send error: ",e);
        		response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
        		
        		try {
					NRUtils.sendERROR(request.getRemoteAddr(),Integer.parseInt(nrport),Integer.parseInt(nrtreeId));
				} catch (IOException e1) {
					LOG.error("Error while sending ERROR: ",e1);
				}
	            return;
        	}
        }
        else{
	        if(!file.exists()){
	            response.setStatus(HttpServletResponse.SC_NOT_FOUND);
	            return;
	        }
	        if(file.isDirectory()){
	            response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
	            return;
	        }
	
	        response.setContentType(URLConnection.guessContentTypeFromName(filePath));
	        
	        FileInputStream in = new FileInputStream(file);
	        OutputStream out = response.getOutputStream();
	        IOUtils.copy(in, out);
	        
	        /*
	        final RateLimiter rateLimiter = RateLimiter.create(10*1024,1,TimeUnit.SECONDS); // 10 Kbps
	        
	        byte[] buffer = new byte[DEFAULT_BUFFER_SIZE];
	        int n = 0;
	        while (-1 != (n = in.read(buffer))) {
	        	rateLimiter.acquire(n*8);
	            out.write(buffer, 0, n);
	        }
	        in.close();
	        */
        }
    }

    @Override
    public void doPost(HttpServletRequest request, HttpServletResponse response)
            throws IOException {
        if(!ServletFileUpload.isMultipartContent(request)){
            response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
            return;
        }
        try {
            List<FileItem> files = uploadHandler.parseRequest(request);
            if(files.size() == 0){
                response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
                return;
            }
            FileItem fileItem = files.get(0);
            if(!fileItem.getContentType().equals(Constants.CLASS_CONTENT_TYPE) || fileItem.isFormField()){
                response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
                return;
            }
            String folderName = baseDir + request.getRequestURI().replaceAll("/", Constants.FILE_SEPARATOR);
            File folder = new File(folderName);
            if(folder.exists() && !folder.isDirectory()){
                response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
                return;
            }
            if(!folder.exists()){
                folder.mkdirs();
            }
            fileItem.write(new File(folderName + Constants.FILE_SEPARATOR + fileItem.getName()));
        } catch (Exception e) {
            response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
            return;
        }
    }
}
