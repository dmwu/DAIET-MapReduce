package netreducer;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Heartbeat implements Runnable {
	
	private Logger LOG = LoggerFactory.getLogger(Heartbeat.class);
	private String dIP;
	private int dPort;
	private int treeId;
	
	public Heartbeat(String dIP, int dPort, int treeId){
		this.dIP=dIP;
		this.dPort=dPort;
		this.treeId=treeId;
	}
	
    public void run() { 
    	try {
			NRUtils.sendKeepalive(dIP, dPort, treeId);
		} catch (IOException e) {
			LOG.error("Heartbeat error!", e);
		} 
    }
};