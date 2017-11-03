package netreducer;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.apache.commons.codec.binary.Hex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.RateLimiter;

import edu.cmu.courses.simplemr.mapreduce.Pair;

public class NRUtils {
	
	private static Logger LOG = LoggerFactory.getLogger(NRUtils.class);
	
	/**
	 * Send ERROR UDP packet.
	 * @param dIP Destination IP of datagrams.
	 * @param dPort Destination port of datagrams.
	 * @throws IOException
	 */
	static public void sendERROR (String dIP, int dPort, int treeId) throws IOException{		
		
		ByteBuffer dgramBuffer = ByteBuffer.wrap(new byte[5]);		
		DatagramSocket socket = new DatagramSocket();
		
		/* ERROR packet */
		dgramBuffer.put(0, (byte)0xff); // frame_type
		dgramBuffer.putInt(1,treeId); // tree_id
		
		DatagramPacket packet = new DatagramPacket(dgramBuffer.array(),5, InetAddress.getByName(dIP), dPort);
		socket.send(packet);
		
		socket.close();
	}
	
	static public void sendKeepalive (String dIP, int dPort, int treeId) throws IOException{		
		
		ByteBuffer dgramBuffer = ByteBuffer.wrap(new byte[5]);		
		DatagramSocket socket = new DatagramSocket();
		
		/* KEEPALIVE packet */
		dgramBuffer.put(0, (byte)0x05); // frame_type
		dgramBuffer.putInt(1,treeId); // tree_id
		
		DatagramPacket packet = new DatagramPacket(dgramBuffer.array(),5, InetAddress.getByName(dIP), dPort);
		socket.send(packet);
		
		socket.close();
	}
	
	/**
	 * Send END UDP packet.
	 * @param dIP Destination IP of datagrams.
	 * @param dPort Destination port of datagrams.
	 * @throws IOException
	 * @throws InterruptedException 
	 */
	static public void sendEND (String dIP, int dPort, int treeId) throws IOException, InterruptedException{		
		
		ByteBuffer dgramBuffer = ByteBuffer.wrap(new byte[5]);		
		DatagramSocket socket = new DatagramSocket();
		
		/* END packet */
		dgramBuffer.put(0, (byte)0x01); // frame_type
		dgramBuffer.putInt(1,treeId); // tree_id
		
		DatagramPacket packet = new DatagramPacket(dgramBuffer.array(),5, InetAddress.getByName(dIP), dPort);
		
		/* Sleep 1 sec. to avoid reordering */
		Thread.sleep(1000);
		socket.send(packet);
		
		socket.close();
	}
	
	/**
	 * Write output file in the netreducer format. If the file already exists, it is overwritten.
	 * @param entries Map of key,value pairs to write in the file.
	 * @param outputFilename Output file name.
	 * @throws IOException
	 */
	static public void writeFile(Map<String,Integer> entries, String outputFilename) throws IOException{
		
		byte[] entryKey;
		String stringKey;
		
		OutputStream outputStream = new BufferedOutputStream(new FileOutputStream(new File(outputFilename)));
		
		// File preamble
		outputStream.write(ByteBuffer.allocate(4).putInt(entries.size()).array());
		
		Iterator<Map.Entry<String,Integer>> it = entries.entrySet().iterator();
		
	    while (it.hasNext()) {
	    	Map.Entry<String,Integer> pair = (Map.Entry<String,Integer>)it.next();
	    	
	    	stringKey = pair.getKey();
	    	
	    	if (stringKey.length()>16)
	    		stringKey = stringKey.substring(0, 16);
	    	
	    	entryKey = stringKey.getBytes("UTF-8");
	    		    	
	    	outputStream.write(entryKey);
	    	
	    	if (entryKey.length<16){
	    		outputStream.write(new byte[16-entryKey.length]);
	    	}
	    	
	    	outputStream.write(ByteBuffer.allocate(4).putInt(pair.getValue()).array());
	    }
	    
		outputStream.close();
	}
	
	/**
	 * Write output file in the netreducer format. If the file already exists, it is overwritten.
	 * @param entries PriorityQueue of key,value pairs to write in the file.
	 * @param outputFilename Output file name.
	 * @throws IOException
	 */
	static public void writeFile(PriorityQueue<Pair<String,Integer>> entries, String outputFilename) throws IOException{
		
		byte[] entryKey;
		String stringKey;
		
		OutputStream outputStream = new BufferedOutputStream(new FileOutputStream(new File(outputFilename)));
		
		// File preamble
		outputStream.write(ByteBuffer.allocate(4).putInt(entries.size()).array());
		
		Iterator<Pair<String,Integer>> it = entries.iterator();
		
	    while (it.hasNext()) {
	    	Pair<String,Integer> pair = it.next();
	    	
	    	stringKey = pair.getKey();
	    	
	    	if (stringKey.length()>16)
	    		stringKey = stringKey.substring(0, 16);
	    	
	    	entryKey = stringKey.getBytes("UTF-8");
	    		    	
	    	outputStream.write(entryKey);
	    	
	    	if (entryKey.length<16){
	    		outputStream.write(new byte[16-entryKey.length]);
	    	}
	    	
	    	outputStream.write(ByteBuffer.allocate(4).putInt(pair.getValue()).array());
	    }
	    
		outputStream.close();
	}
	
	/**
	 * Read file in the netreducer format.
	 * @param inputFilename Input file name.
	 * @param entries Output parameter. Map of key,value pairs to write in.
	 * @throws Exception
	 */
	static public void readFile(String inputFilename, Map<String,Integer> entries) throws Exception{
		
		File inputFile = new File(inputFilename);
		InputStream inputStream = new BufferedInputStream(new FileInputStream(inputFile));

		int totalNumberOfEntries = 0;
		int readEntries = 0;
		int read = 0;
		boolean eof = false;
		
		ByteBuffer intBuffer = ByteBuffer.wrap(new byte[4]);
		ByteBuffer keyBuffer = ByteBuffer.wrap(new byte[16]);
		ByteBuffer valueBuffer = ByteBuffer.wrap(new byte[4]);
		String key = null;
		Integer value = null;
		
		if (inputFile.length()<4){
			inputStream.close();
			throw new Exception("Missing preamble in file: " + inputFilename);
		}
		
		if (readBytes(inputStream, intBuffer.array())!=4)
			throw new Exception("Missing number of entries in file: " + inputFilename);
		
		totalNumberOfEntries = intBuffer.getInt(0);
		
		while (readEntries < totalNumberOfEntries && !eof){
			
			read = readBytes(inputStream, keyBuffer.array());
			
			if(read>0){
				
				key = new String(keyBuffer.array(), "UTF-8");
				
				/* Remove trailing zeros */
				key = key.trim();
				
				read = readBytes(inputStream, valueBuffer.array());
				
				if(read>0){
					
					value = new Integer(valueBuffer.getInt(0));
					
					readEntries++;
					
					entries.put(key, value);
					
				} else if (read == 0){
					LOG.error(inputFilename+" contains less entries than declared.");
					eof=true;
				}
			} else if (read == 0){
				LOG.error(inputFilename+" contains less entries than declared.");
				eof=true;
			}
		}
		
		inputStream.close();
	}
	
	/**
	 * Read file in the netreducer format.
	 * @param inputFilename Input file name.
	 * @param entries Output parameter. PriorityQueue of key,value pairs to write in.
	 * @throws Exception
	 */ 
	static public void readFile(String inputFilename, PriorityQueue<Pair<String,Integer>> entries) throws Exception{
		
		File inputFile = new File(inputFilename);
		InputStream inputStream = new BufferedInputStream(new FileInputStream(inputFile));

		int totalNumberOfEntries = 0;
		int readEntries = 0;
		int read = 0;
		boolean eof = false;
		
		ByteBuffer intBuffer = ByteBuffer.wrap(new byte[4]);
		ByteBuffer keyBuffer = ByteBuffer.wrap(new byte[16]);
		ByteBuffer valueBuffer = ByteBuffer.wrap(new byte[4]);
		String key = null;
		Integer value = null;
		
		if (inputFile.length()<4){
			inputStream.close();
			throw new Exception("Missing preamble in file: " + inputFilename);
		}
		
		if (readBytes(inputStream, intBuffer.array())!=4)
			throw new Exception("Missing number of entries in file: " + inputFilename);
		
		totalNumberOfEntries = intBuffer.getInt(0);
		
		while (readEntries < totalNumberOfEntries && !eof){
			
			read = readBytes(inputStream, keyBuffer.array());
			
			if(read>0){
				
				key = new String(keyBuffer.array(), "UTF-8");
				
				/* Remove trailing zeros */
				key = key.trim();
				
				read = readBytes(inputStream, valueBuffer.array());
				
				if(read>0){
					
					value = new Integer(valueBuffer.getInt(0));
					
					readEntries++;
					
					entries.add(new Pair<String, Integer>(key, value));
					
				} else if (read == 0){
					LOG.error(inputFilename+" contains less entries than declared.");
					eof=true;
				}
			} else if (read == 0){
				LOG.error(inputFilename+" contains less entries than declared.");
				eof=true;
			}
		}
		
		inputStream.close();
	}
	
	/**
	 * Send file using UDP datagrams in the netreducer format.
	 * 
	 * @param inputFilename File to send.
	 * @param sIP Source IP of datagrams. 
	 * @param sPort Source port of datagrams.
	 * @param dIP Destination IP of datagrams.
	 * @param dPort Destination port of datagrams.
	 * @param treeId The Tree ID.
	 * @throws Exception
	 */
	static public void sendFile (String inputFilename, String sIP, int sPort, String dIP, int dPort, int treeId) throws Exception{		
		
		final int MAX_ENTRIES_PER_DGRAM = 10; 
		final RateLimiter rateLimiter = RateLimiter.create(10*1024,1,TimeUnit.SECONDS); // 10 Kbps

		/* Keepalive thread */
		final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
		final ScheduledFuture<?> heartbeatHandle = scheduler.scheduleAtFixedRate(new Heartbeat(dIP, dPort, treeId), 0, 10, TimeUnit.SECONDS);
		
		File inputFile = new File(inputFilename);
		InputStream inputStream = new BufferedInputStream(new FileInputStream(inputFile));
		DatagramSocket socket =null;
		
		int totalNumberOfEntries =0;
		int sentEntries = 0;
		int read =0;
		boolean eof = false;
		
		ByteBuffer intBuffer = ByteBuffer.wrap(new byte[4]);
		ByteBuffer dgramBuffer = ByteBuffer.wrap(new byte[9+20*MAX_ENTRIES_PER_DGRAM]);
		
		DatagramPacket packet = null;
		
		if (inputFile.length()<4){
			inputStream.close();
			throw new Exception("Missing preamble in file: " + inputFilename);
		}
		
		if (readBytes(inputStream, intBuffer.array())!=4)
			throw new Exception("Missing number of entries in file: " + inputFilename);
		
		totalNumberOfEntries = intBuffer.getInt(0);
		
		if (sPort==0) 
			socket = new DatagramSocket();
		else if (sIP==null)
			socket = new DatagramSocket(sPort);
		else
			socket = new DatagramSocket(sPort, InetAddress.getByName(sIP));
		
		while (sentEntries < totalNumberOfEntries && !eof){
			
			read = readBytes(inputStream, dgramBuffer.array(), 9);
			
			if(read>0){
				
				/* Preamble */
				dgramBuffer.put(0, (byte)0x00); // frame_type
				dgramBuffer.putInt(1,read/20); // number_of_entries
				dgramBuffer.putInt(5,treeId); // tree_id
				
				packet = new DatagramPacket(dgramBuffer.array(),9+read, InetAddress.getByName(dIP), dPort);
				
				rateLimiter.acquire(packet.getLength()*8);
				socket.send(packet);
				
				sentEntries += read/20;
				
			} else if (read == 0){
				LOG.error(inputFilename+" contains less entries than declared.");
				eof=true;
			}
			
		}
		
		inputStream.close();
		
		/* END packet */
		dgramBuffer.put(0, (byte)0x01); // frame_type
		dgramBuffer.putInt(1,treeId); // tree_id
		
		packet = new DatagramPacket(dgramBuffer.array(),5, InetAddress.getByName(dIP), dPort);
		
		/* Sleep 1 sec. to avoid reordering */
		Thread.sleep(1000);
		socket.send(packet);
		
		heartbeatHandle.cancel(false);
		socket.close();
	}
	
	/**
	 * Send file using UDP datagrams in the netreducer format.
	 * 
	 * @param inputFilename File to send.
	 * @param dIP Destination IP of datagrams.
	 * @param dPort Destination port of datagrams.
	 * @param treeId The Tree ID.
	 * @throws Exception
	 */
	static public void sendFile (String inputFilename, String dIP, int dPort, int treeId) throws Exception{		
		sendFile(inputFilename, null, 0, dIP, dPort, treeId);
	}
	
	/**
	 * Receive file with UDP datagrams in the netreducer format.
	 * @param outputFilename File name.
	 * @param treeId The expected Tree ID.
	 * @param rcvIP Listening IP address.
	 * @param rcvPort Listening port.
	 * @return True if correctly received.
	 * @throws IOException
	 */
	static public boolean receiveFile (String outputFilename, int treeId, String rcvIP, int rcvPort) throws IOException{
		
		DatagramPacket packet = null;
		DatagramSocket socket = new DatagramSocket(rcvPort, InetAddress.getByName(rcvIP));
		
		OutputStream outputStream = new BufferedOutputStream(new FileOutputStream(new File(outputFilename)));
		
		ByteArrayInputStream bais = null;
		DataInputStream dis = null;
		
		byte[] payload = null;
		int actualPayloadSize = 0;
		byte frameType = 0;
		int rcvTreeId = 0;
		int numberOfEntries = 0;
		int totalNumberOfEntries = 0;
		byte[] recvBuffer = new byte[1460];
		String previousPkt=null;
		
		boolean stop = false;
		while (!stop){
		
			packet = new DatagramPacket(recvBuffer, recvBuffer.length);
			
			socket.setSoTimeout(60000); //1 minute
			try {
				socket.receive(packet);
			
				/* TO DELETE */
				previousPkt = new String(Hex.encodeHex(Arrays.copyOfRange(packet.getData(), 0, packet.getLength())));
				
				bais = new ByteArrayInputStream(packet.getData());
				dis = new DataInputStream(bais);
				
				if (packet.getLength() >= 5){
	
					frameType = dis.readByte();
					
					if (frameType==(byte)0x00){
						// DATA packet
						
						if (packet.getLength() >= 9){
							
							numberOfEntries = dis.readInt();
							rcvTreeId = dis.readInt();
							
							if (rcvTreeId==treeId){
								
								payload = new byte[packet.getLength()-9];
								
								if (totalNumberOfEntries==0){
									// First packet
									// File preamble
									outputStream.write(ByteBuffer.allocate(4).putInt(totalNumberOfEntries).array());
								}
								
								actualPayloadSize = readBytes(dis, payload);
								if (actualPayloadSize!=numberOfEntries*20){
									LOG.warn("Received a DATA datagram with a number_of_entries: " + numberOfEntries + " and payload size: "+ actualPayloadSize);
									numberOfEntries = (packet.getLength()-9)/20;
								}
															
								outputStream.write(payload, 0, actualPayloadSize);
								
								totalNumberOfEntries+=numberOfEntries;
								
							} else {
								LOG.error("Received a DATA datagram from "+packet.getAddress()+":"+packet.getPort()+" with an unexpected tree ID: "+ rcvTreeId);
							}	
						} else {
							LOG.error("Received a DATA datagram from "+packet.getAddress()+":"+packet.getPort()+" with "+ packet.getLength() +" bytes.");
						}
						
					} else if (frameType==(byte)0x01){
						// END packet
						rcvTreeId = dis.readInt();
						
						if (rcvTreeId==treeId){
							stop = true;
						} else {
							LOG.error("Received a END datagram from "+packet.getAddress()+":"+packet.getPort()+" with an unexpected tree ID: "+ rcvTreeId);
						}
					} else if (frameType==(byte)0xff){
						// ERROR packet
						rcvTreeId = dis.readInt();
						if (rcvTreeId==treeId){
							stop = true;
							LOG.error("Received error datagram from "+packet.getAddress()+":"+packet.getPort()+".");
						} else {
							LOG.error("Received a ERROR datagram from "+packet.getAddress()+":"+packet.getPort()+" with an unexpected tree ID: "+ rcvTreeId);
						}
						
					} else if (frameType==(byte)0x05){
						// Heartbeat
						LOG.info("X from "+packet.getAddress()+":"+packet.getPort()+"");
					} else {
						socket.close();
						outputStream.close();
						dis.close();
						LOG.error("Received a datagram from "+packet.getAddress()+":"+packet.getPort()+" with a wrong frame_type: "+ Hex.encodeHexString(new byte[] {frameType}));
						return false;
					}
				} else {
					socket.close();
					LOG.error("Received a datagram from "+packet.getAddress()+":"+packet.getPort()+" with "+ packet.getLength() +" bytes.");
				}
				
			}catch (SocketTimeoutException ste){
				
				if (previousPkt==null)
					LOG.error("UDP socket "+rcvIP+":"+rcvPort+" timed out. Previous packet null");
				else
					LOG.warn("UDP socket "+rcvIP+":"+rcvPort+" timed out.");
				
				stop = true;
			}
			
			if (dis!=null)
				dis.close();
		}
		
		socket.close();
		outputStream.close();
		
		/* Update number of entries */
		RandomAccessFile raf = new RandomAccessFile(outputFilename, "rw");
		raf.seek(0); 
		raf.write(ByteBuffer.allocate(4).putInt(totalNumberOfEntries).array());		
		raf.close();
		
		return true;
	}		
	
	/**
	 * Read  from the input stream in a byte array.
	 * @param is InputStream to read from.
	 * @param result Byte array to write in.
	 * @param offset Starting offset in the byte array.
	 * @return The number of bytes read.
	 * @throws IOException
	 */
	static private int readBytes(InputStream is, byte[] result, int offset) throws IOException{

		int totalBytesRead =0;
		int bytesRemaining = 0; 
		int bytesRead = 0;
	
		while(totalBytesRead < result.length - offset){
	
			bytesRemaining = result.length - offset - totalBytesRead;
	
			bytesRead = is.read(result, totalBytesRead + offset, bytesRemaining);
			if (bytesRead > 0){
				totalBytesRead += bytesRead;
			} else if (bytesRead <0) {
				return totalBytesRead;
			}          
		}
	
		return totalBytesRead;
	}
	
	/**
	 * Read  from the input stream in a byte array.
	 * @param is InputStream to read from.
	 * @param result Byte array to write in.
	 * @return The number of bytes read.
	 * @throws IOException
	 */
	static private int readBytes(InputStream is, byte[] result) throws IOException{
		return readBytes(is, result,0);
	}
}