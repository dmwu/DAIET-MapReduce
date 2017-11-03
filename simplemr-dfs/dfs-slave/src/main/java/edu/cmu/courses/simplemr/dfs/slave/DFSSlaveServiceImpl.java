package edu.cmu.courses.simplemr.dfs.slave;

import edu.cmu.courses.simplemr.dfs.DFSSlaveService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

/**
 * Implementation of service class. The class contains a DFSSlave
 * instance that handle data request from user.
 *
 * @author Jian Fang(jianf)
 * @author Fangyu Gao(fangyug)
 */

public class DFSSlaveServiceImpl extends UnicastRemoteObject implements DFSSlaveService {

    private static Logger LOG = LoggerFactory.getLogger(DFSSlaveServiceImpl.class);

    private DFSSlave slave;

    protected DFSSlaveServiceImpl(DFSSlave slave) throws RemoteException {
        this.slave = slave;
    }

    public byte[] read(long chunkId, long offset, int size) throws RemoteException {
        try {
            return slave.read(chunkId, offset, size);
        } catch (IOException e) {
            return null;
        }
    }

    public boolean write(long chunkId, long offset, int size, byte[] data) throws RemoteException {
        try {
            slave.write(chunkId, offset, size, data);
            return true;
        } catch (IOException e) {
            return false;
        }
    }

    public void delete(long chunkId) throws RemoteException {
        LOG.debug("delete chunk " + chunkId);
        slave.delete(chunkId);
    }

    public long[] linesOffset(long chunkId) throws RemoteException {
        try {
            return slave.linesOffset(chunkId);
        } catch (IOException e) {
            throw new RemoteException("can't access chunk " + chunkId);
        }
    }
}
