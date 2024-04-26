package distributed.system.zookeeper;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.util.List;
import java.util.Objects;

public class WatcherDemo implements Watcher {
    private static final String ZOOKEEPER_ADDRESS = "localhost:2181";
    private static final int SESSION_TIMEOUT = 3000; // in milliseconds
    private static final String TARGATE_ZNODE = "/target_znode";
    private ZooKeeper zookeeper;

    public static void main(String[] args) {
        WatcherDemo watcherDemo = new WatcherDemo();
        watcherDemo.connectToZookeeper();
        watcherDemo.run();
        watcherDemo.close();
        System.out.println("Disconnected from Zookeeper, exiting application");
    }

    private void run() {
        synchronized (zookeeper) {
            try {
                zookeeper.wait();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private void connectToZookeeper() {
        try {
            this.zookeeper = new ZooKeeper(ZOOKEEPER_ADDRESS, SESSION_TIMEOUT, this);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void close() {
        try {
            zookeeper.close();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void watchTargetZnode() {
        try {
            final var stat = zookeeper.exists(TARGATE_ZNODE, this); //returns metadata of the znode
            if (stat == null) {    //znode does not exist
                return;
            }
            final var data = zookeeper.getData(TARGATE_ZNODE, this, stat);
            if (Objects.nonNull(data)) {
                final var dataString = new String(data);
                System.out.println("Data: " + dataString);
            }
            final var children = zookeeper.getChildren(TARGATE_ZNODE, this);//watch for children changes (new children, deleted children)
            System.out.println("Children: " + children);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void process(final WatchedEvent watchedEvent) {
        System.out.println("watchedEvent = " + watchedEvent);
        switch (watchedEvent.getType()) {
            case None:
                if (watchedEvent.getState() == Event.KeeperState.SyncConnected) {
                    System.out.println("Successfully connected to Zookeeper");
                } else {
                    synchronized (this) {
                        System.out.println("Disconnected from Zookeeper event");
                        notifyAll();
                    }
                }
                break;
            case NodeCreated:
                System.out.println(TARGATE_ZNODE + " was created");
                break;
            case NodeDeleted:
                System.out.println(TARGATE_ZNODE + " was deleted");
                break;
            case NodeDataChanged:
                System.out.println(TARGATE_ZNODE + " data changed");
                break;
            case NodeChildrenChanged:
                System.out.println(TARGATE_ZNODE + " children changed");
                break;
        }
        watchTargetZnode();
    }
}
