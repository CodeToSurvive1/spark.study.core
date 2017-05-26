package io.github.codetosurvive.zookeeper;

import java.io.IOException;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.ConnectionLossException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;

import org.slf4j.*;

public class Client implements Watcher {

	private static final Logger log = LoggerFactory.getLogger(Client.class);

	private ZooKeeper zk;

	private String hostPort;

	Client(String hostPort) {
		this.hostPort = hostPort;
	}

	void startZK() throws IOException {
		zk = new ZooKeeper(hostPort, 15000, this);
	}

	@Override
	public void process(WatchedEvent event) {
		log.info(event.toString());
	}

	String quequeCommand(String comand) throws KeeperException, InterruptedException {
		while (true) {
			try {
				String name = zk.create("/tasks/task-", comand.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,
						CreateMode.PERSISTENT_SEQUENTIAL);
				return name;
			} catch (ConnectionLossException e) {

			}
		}
	}

	public static void main(String args[]) throws KeeperException, InterruptedException, IOException {
		Client c = new Client(args[0]);
		c.startZK();

		String name = c.quequeCommand(args[1]);

		System.out.println("created " + name);
	}

}
