package distributed.system;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Aggregator {
    private final WebClient webClient;

    public Aggregator() {
        this.webClient = new WebClient();
    }

    public List<String> sendTaskToWorkers(List<String> workerAddresses, List<String> tasks) {
        CompletableFuture<String>[] futures = new CompletableFuture[workerAddresses.size()];
        for (int i = 0; i < workerAddresses.size(); i++) {
            String address = workerAddresses.get(i);
            String task = tasks.get(i);
            futures[i] = webClient.sendTask(address, task.getBytes());
        }
        return Stream.of(futures).map(CompletableFuture::join).collect(Collectors.toList());
    }
}
