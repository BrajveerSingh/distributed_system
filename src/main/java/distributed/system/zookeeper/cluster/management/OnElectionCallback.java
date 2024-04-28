package distributed.system.zookeeper.cluster.management;

public interface OnElectionCallback {
    void onElectedToBeLeader();
    void onWorker();
}
