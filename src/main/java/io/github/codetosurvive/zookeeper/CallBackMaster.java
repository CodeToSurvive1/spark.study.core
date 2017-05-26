package io.github.codetosurvive.zookeeper;

import java.io.IOException;
import java.util.Random;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.hadoop.util.ZKUtil.ZKAuthInfo;
import org.apache.zookeeper.AsyncCallback.DataCallback;
import org.apache.zookeeper.AsyncCallback.StringCallback;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CallBackMaster implements Watcher {

	private static final Logger log = LoggerFactory.getLogger(CallBackMaster.class);
	private String hostPort;

	private ZooKeeper zk;

	private Random random = new Random(this.hashCode());

	private String serverId = Integer.toHexString(random.nextInt());

	private boolean isLeader = false;

	CallBackMaster(String hostPort) {
		this.hostPort = hostPort;
	}

	void startZK() throws IOException {
		zk = new ZooKeeper(hostPort, 15000, this);
	}

	void stopZK() throws InterruptedException {
		zk.close();
	}

	@Override
	public void process(WatchedEvent event) {
		System.out.println(event);
	}

	void runForMaster() {
		zk.create("/master", serverId.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL,
				masterCreateCallBack, null);
	}

	StringCallback masterCreateCallBack = new StringCallback() {

		@Override
		public void processResult(int rc, String path, Object ctx, String name) {
			switch (Code.get(rc)) {
			case CONNECTIONLOSS:
				checkMaster();
				break;
			case OK:
				isLeader = true;
				break;
			default:
				isLeader = false;
				break;
			}
		}
	};

	DataCallback masterCheckCallBack = new DataCallback() {

		@Override
		public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
			switch (Code.get(rc)) {
			case CONNECTIONLOSS:
				checkMaster();
				return;
			case NONODE:
				runForMaster();
				return;
			}
		}
	};

	void checkMaster() {
		zk.getData("/master", false, masterCheckCallBack, null);
	}

	void createParent(String path, byte[] data) {
		zk.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT, createParentCallBack, data);
	}

	StringCallback createParentCallBack = new StringCallback() {

		@Override
		public void processResult(int rc, String path, Object ctx, String name) {
			switch (Code.get(rc)) {
			case CONNECTIONLOSS:
				createParent(path, (byte[]) ctx);
				break;
			case OK:
				log.info("parent created");
				break;
			case NODEEXISTS:
				log.warn("parent already registered :" + path);
				break;
			default:
				log.error("something went wrong:" + KeeperException.create(Code.get(rc), path));
				break;
			}
		}
	};

	public void bootstrap(){
		createParent("/workers",new byte[0]);
		createParent("/assign", new byte[0]);
		createParent("/tasks", new byte[0]);
		createParent("/status", new byte[0]);
	}
	public static void main(String[] args) {
	}

}
