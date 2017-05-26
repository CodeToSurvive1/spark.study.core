package io.github.codetosurvive.zookeeper;

import java.io.IOException;
import java.util.Random;

import org.apache.zookeeper.AsyncCallback.StatCallback;
import org.apache.zookeeper.AsyncCallback.StringCallback;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Worker implements Watcher {

	private static final Logger log = LoggerFactory.getLogger(Worker.class);

	private ZooKeeper zk;

	private Random random = new Random(this.hashCode());

	private String serverId = Integer.toHexString(random.nextInt());

	private String hostPort;

	Worker(String hostPort) {
		this.hostPort = hostPort;
	}

	@Override
	public void process(WatchedEvent event) {
		log.info(event.toString() + "," + hostPort);
	}

	void startZK() throws IOException {
		zk = new ZooKeeper(hostPort, 15000, this);
	}

	void register() {
		zk.create("/workers/worker-" + serverId, "Idle".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL,
				createWorkerCallBack, null);
	}

	StringCallback createWorkerCallBack = new StringCallback() {

		@Override
		public void processResult(int rc, String path, Object ctx, String name) {
			switch (Code.get(rc)) {
			case CONNECTIONLOSS:
				register();
				break;
			case OK:
				log.info("registered successfully:" + serverId);
				break;
			case NODEEXISTS:
				log.warn("already registered:" + serverId);
			default:
				log.error("something went wrong:" + KeeperException.create(Code.get(rc), path));
				break;
			}
		}
	};

	private String status;

	void setStatus(String status) {
		this.status = status;
		updateStatus(status);
	}

	synchronized private void updateStatus(String status) {
		if (this.status.equals(status)) {
			zk.setData("/workers/worker-" + serverId, status.getBytes(), -1, statusUpdateCallBack, status);
		}
	}

	StatCallback statusUpdateCallBack = new StatCallback() {

		@Override
		public void processResult(int rc, String path, Object ctx, Stat stat) {
			switch (Code.get(rc)) {
			case CONNECTIONLOSS:
				updateStatus((String) ctx);
				return;
			default:
				break;
			}
		}
	};

	public static void main(String args[]) throws Exception {
		Worker w = new Worker(args[0]);

		w.startZK();

		w.register();

		Thread.sleep(30000);

	}
}
