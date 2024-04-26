package distributed.system.zookeeper;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;

public class LeaderElection implements Watcher {
    private static final Logger LOGGER = LoggerFactory.getLogger(LeaderElection.class);
    private static final String ZOOKEEPER_ADDRESS = "localhost:2181";
    private static final int SESSION_TIMEOUT = 3000; // in milliseconds
    private static final String ELECTION_NAMESPACE = "/election";
    private ZooKeeper zookeeper;
    private String currentZnodeName;

    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
        LeaderElection leaderElection = new LeaderElection();
        leaderElection.connectToZookeeper();

        leaderElection.volunteerForLeadership();
        leaderElection.reElectLeader();
        leaderElection.run();
        leaderElection.close();
        LOGGER.info("Disconnected from Zookeeper, exiting application");
    }

    private void electLeader() throws InterruptedException, KeeperException {
        final var children = zookeeper.getChildren(ELECTION_NAMESPACE, false);
        Collections.sort(children);
        final var smallestChild = children.get(0);
        if (smallestChild.equals(currentZnodeName)) {
            LOGGER.info("I am the leader");
        } else {
            LOGGER.info("I am not the leader. The leader is {}", smallestChild);
        }
    }


    public void volunteerForLeadership() throws InterruptedException, KeeperException {
        String znodePrefix = ELECTION_NAMESPACE + "/c_";  //c stands for candidate
        String znodeFullPath =
                zookeeper.create(
                        znodePrefix,
                        new byte[]{},
                        ZooDefs.Ids.OPEN_ACL_UNSAFE,
                        CreateMode.EPHEMERAL_SEQUENTIAL
                );
        LOGGER.info("Znode name: {}", znodeFullPath);
        this.currentZnodeName = znodeFullPath.replace(ELECTION_NAMESPACE + "/", "");
    }

    private void reElectLeader() throws InterruptedException, KeeperException {
        Stat predecessorStat = null;
        String predecessorZnodeName = "";
        while (predecessorStat == null) {
            final var children = zookeeper.getChildren(ELECTION_NAMESPACE, false);
            Collections.sort(children);
            final var smallestChild = children.get(0);
            if (smallestChild.equals(currentZnodeName)) {
                LOGGER.info("I am the leader");
                return;
            } else {
                LOGGER.info("I am not the leader. The leader is {}", smallestChild);
                int predecessorIndex = Collections.binarySearch(children, currentZnodeName) - 1;
                predecessorZnodeName = children.get(predecessorIndex);
                predecessorStat = zookeeper.exists(ELECTION_NAMESPACE + "/" + predecessorZnodeName, this);
            }
        }
        LOGGER.info("Watching znode {}", predecessorZnodeName);
    }

    private void close() {
        synchronized (zookeeper) {
            try {
                zookeeper.close();
            } catch (InterruptedException e) {
                LOGGER.error("Zookeeper session failed", e);
                Thread.currentThread().interrupt();
            }
        }
    }

    private void run() {
        synchronized (zookeeper) {
            try {
                zookeeper.wait();
            } catch (InterruptedException e) {
                LOGGER.error("Zookeeper session failed", e);
                Thread.currentThread().interrupt();
            }
        }
    }

    public void connectToZookeeper() throws IOException {
        LOGGER.info("Connecting to Zookeeper at {}", ZOOKEEPER_ADDRESS);
        this.zookeeper = new ZooKeeper(ZOOKEEPER_ADDRESS, SESSION_TIMEOUT, this);
    }

    @Override
    public void process(final WatchedEvent watchedEvent) {
        LOGGER.info("Received watched event:{}", watchedEvent);
        switch (watchedEvent.getType()) {
            case None:
                if (watchedEvent.getState() == Event.KeeperState.SyncConnected) {
                    LOGGER.info("Successfully connected to Zookeeper");
                } else {
                    synchronized (zookeeper) {
                        LOGGER.info("Disconnected from Zookeeper event");
                        zookeeper.notifyAll();
                    }
                }
                break;
            case NodeDeleted:
                try {
                    reElectLeader();
                } catch (InterruptedException | KeeperException e) {
                    LOGGER.error("Failed to re-elect leader", e);
                }
                break;
        }
    }
}
