package io.github.codetosurvive.zookeeper;

import java.io.IOException;
import java.util.Date;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

public class AdminClient implements Watcher {

	private ZooKeeper zk;

	private String hostPort;

	public AdminClient(String hostPort) {
		this.hostPort = hostPort;
	}

	void startZK() throws IOException {
		zk = new ZooKeeper(hostPort, 15000, this);
	}

	void listState() throws KeeperException, InterruptedException {
		try {
			Stat stat = new Stat();

			byte masterData[] = zk.getData("/master", false, stat);

			Date startDate = new Date(stat.getCtime());

			System.out.println("master :" + new String(masterData) + " since " + startDate);

		} catch (NoNodeException e) {
			System.out.println("no master");
		}

		System.out.println("workers:");

		for (String w : zk.getChildren("/workers", false)) {
			byte[] data = zk.getData("/workers/" + w, false, null);
			String state = new String(data);

			System.out.println("\t" + w + ":" + state);
		}

		System.out.println("tasks:");

		for (String t : zk.getChildren("/assign", false)) {
			System.out.println("\t" + t);
		}
	}

	@Override
	public void process(WatchedEvent event) {
		System.out.println(event.toString());
	}
	
	public static void main(String[] args) throws KeeperException, InterruptedException, IOException {
		AdminClient ac = new AdminClient(args[0]);
		
		ac.startZK();
		
		ac.listState();
	}

}
