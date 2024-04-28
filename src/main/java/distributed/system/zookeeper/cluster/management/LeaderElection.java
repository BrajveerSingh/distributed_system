package distributed.system.zookeeper.cluster.management;

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
    private static final String ELECTION_NAMESPACE = "/election";
    private ZooKeeper zookeeper;
    private String currentZnodeName;
    private final OnElectionCallback onElectionCallback;

    public LeaderElection(final ZooKeeper zookeeper, OnElectionCallback onElectionCallback) {
        this.zookeeper = zookeeper;
        this.onElectionCallback = onElectionCallback;
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

    public void reElectLeader() throws InterruptedException, KeeperException {
        Stat predecessorStat = null;
        String predecessorZnodeName = "";
        while (predecessorStat == null) {
            final var children = zookeeper.getChildren(ELECTION_NAMESPACE, false);
            Collections.sort(children);
            final var smallestChild = children.get(0);
            if (smallestChild.equals(currentZnodeName)) {
                LOGGER.info("I am the leader");
                onElectionCallback.onElectedToBeLeader();
                return;
            } else {
                LOGGER.info("I am not the leader. The leader is {}", smallestChild);
                int predecessorIndex = Collections.binarySearch(children, currentZnodeName) - 1;
                predecessorZnodeName = children.get(predecessorIndex);
                predecessorStat = zookeeper.exists(ELECTION_NAMESPACE + "/" + predecessorZnodeName, this);
            }
        }
        onElectionCallback.onWorker();
        LOGGER.info("Watching znode {}", predecessorZnodeName);
    }

    @Override
    public void process(final WatchedEvent watchedEvent) {
        LOGGER.info("Received watched event:{}", watchedEvent);
        switch (watchedEvent.getType()) {
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
