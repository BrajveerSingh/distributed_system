package distributed.system.zookeeper;

import distributed.system.zookeeper.cluster.management.LeaderElection;
import distributed.system.zookeeper.cluster.management.ServiceRegistry;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class Application implements Watcher {
    private static final Logger LOGGER = LoggerFactory.getLogger(Application.class);

    private static final String ZOOKEEPER_ADDRESS = "localhost:2181";
    private static final int SESSION_TIMEOUT = 3000; // in milliseconds
    private static final int DEFAULT_PORT = 8080;
    private ZooKeeper zookeeper;

    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
        int currentServerPort = args.length == 1 ? Integer.parseInt(args[0]) : DEFAULT_PORT;
        Application application = new Application();
        ZooKeeper zookeeper = application.connectToZookeeper();
        ServiceRegistry serviceRegistry = new ServiceRegistry(zookeeper);
        OnElectionAction onElectionAction = new OnElectionAction(serviceRegistry, currentServerPort);
        LeaderElection leaderElection = new LeaderElection(zookeeper, onElectionAction);
        leaderElection.volunteerForLeadership();
        leaderElection.reElectLeader();
        application.run();
        application.close();
        LOGGER.info("Disconnected from Zookeeper, exiting application");
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

    public ZooKeeper connectToZookeeper() throws IOException {
        LOGGER.info("Connecting to Zookeeper at {}", ZOOKEEPER_ADDRESS);
        this.zookeeper = new ZooKeeper(ZOOKEEPER_ADDRESS, SESSION_TIMEOUT, this);
        return zookeeper;
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
        }
    }
}
