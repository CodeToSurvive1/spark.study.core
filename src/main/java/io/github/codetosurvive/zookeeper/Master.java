package io.github.codetosurvive.zookeeper;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.ConnectionLossException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import scala.util.Random;

public class Master implements Watcher {

	private ZooKeeper zk;

	private String hostPort;

	private Random random = new Random(this.hashCode());

	private String serverId = Integer.toHexString(random.nextInt());

	private boolean isLeader = false;

	Master(String hostPort) {
		this.hostPort = hostPort;
	}

	void startZK() throws Exception {
		zk = new ZooKeeper(hostPort, 15000, this);
	}

	@Override
	public void process(WatchedEvent event) {
		System.out.println(event);
	}

	void stopZK() throws Exception {
		zk.close();
	}

	void runForMaster() throws KeeperException, InterruptedException {
		while (true) {

			try {
				zk.create("/master", serverId.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
				
				isLeader = true;
				break;
			} catch (NodeExistsException e) {
				isLeader = false;
				break;
			}catch(ConnectionLossException e){
				
			}
			
			if(checkMaster()) break;
		}
	}

	boolean checkMaster() throws KeeperException, InterruptedException {

		while(true){
			try{
				Stat stat = new Stat();
				byte[] data = zk.getData("/master", false, stat);
				
				isLeader = new String(data).equals(serverId);
				return true;
			}catch(NoNodeException e){
				return false;
			}catch(ConnectionLossException e){
				
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Master m = new Master(args[0]);

		m.startZK();
		
		m.runForMaster();
		
		if(m.isLeader){
			System.out.println("i am the leader");
		}else{
			System.out.println("someone else is the leader");
		}

		Thread.sleep(60000);

		m.stopZK();
		
	}
}
