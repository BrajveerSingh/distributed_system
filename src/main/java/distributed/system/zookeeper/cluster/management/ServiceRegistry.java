package distributed.system.zookeeper.cluster.management;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class ServiceRegistry implements Watcher {
    private static final String REGISTRY_ZNODE = "/service_registry";
    private final ZooKeeper zookeeper;
    private String currentZnode;
    private List<String> allServiceAddresses;

    public ServiceRegistry(ZooKeeper zookeeper) {
        this.zookeeper = zookeeper;
        createServiceRegistryZnode();
    }

    public void registerToCluster(final String metadata) throws InterruptedException, KeeperException {
        this.currentZnode = zookeeper.create(REGISTRY_ZNODE + "/n_", metadata.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        System.out.println("Registered to service registry");
    }

    public void registerForUpdates() {
        try {
            updateAddresses();
        } catch (InterruptedException | KeeperException e) {
            throw new RuntimeException(e);
        }
    }

    public synchronized List<String> getAllServiceAddresses() throws InterruptedException, KeeperException {
        if (Objects.isNull(allServiceAddresses)) {
            updateAddresses();
        }
        return allServiceAddresses;
    }

    public void unregisterFromCluster() throws InterruptedException, KeeperException {
        if(Objects.nonNull(currentZnode) && Objects.nonNull(zookeeper.exists(currentZnode, false))){
            zookeeper.delete(currentZnode, -1);
        }
    }

    private void createServiceRegistryZnode() {
        try {
            if (zookeeper.exists(REGISTRY_ZNODE, false) == null) {
                zookeeper.create(REGISTRY_ZNODE, new byte[]{}, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
        } catch (InterruptedException | KeeperException e) {
            throw new RuntimeException(e);
        }
    }

    private synchronized void updateAddresses() throws InterruptedException, KeeperException {
        final var workerZnodes = zookeeper.getChildren(REGISTRY_ZNODE, this);
        List<String> addresses = new ArrayList<>(workerZnodes.size());
        for (String workerZnode : workerZnodes) {
            final var workerZnodeFullPath = REGISTRY_ZNODE + "/" + workerZnode;
            try {
                Stat stat = zookeeper.exists(workerZnodeFullPath, false);
                if (Objects.isNull(stat)) {
                    continue;
                }
                final var addressBytes = zookeeper.getData(workerZnodeFullPath, false, stat);
                String address = new String(addressBytes);
                addresses.add(address);
            } catch (InterruptedException | KeeperException e) {
                System.out.println("Failed to get data from znode " + workerZnodeFullPath);
            }
        }
        this.allServiceAddresses = Collections.unmodifiableList(addresses);
        System.out.println("The cluster addresses are: " + allServiceAddresses);
    }

    @Override
    public void process(final WatchedEvent watchedEvent) {
        try {
            updateAddresses();
        } catch (InterruptedException | KeeperException e) {
            throw new RuntimeException(e);
        }
    }
}
