package kcleaner.topic.define;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.DescribeLogDirsResult;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.requests.DescribeLogDirsResponse;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;


/**
 * Topic that contains data
 *
 * No test exists as it requires Integration Test, or has to fake a lot
 * */

public class NonEmptyTopic implements ReservedTopic {
    private final AdminClient adminClient;

    private final Set<String> topics;

    public NonEmptyTopic(final AdminClient adminClient) throws ExecutionException, InterruptedException {
        this.adminClient = adminClient;
        topics = new HashSet<>();
        setNonEmptyTopics();
    }

    @Override
    public Set<String> getNames() {
        return Collections.unmodifiableSet(this.topics);
    }

    private void setNonEmptyTopics() throws ExecutionException, InterruptedException {
        final DescribeLogDirsResult logDirsResult = getLogDirsResult();

        for (Map.Entry<Integer, Map<String, DescribeLogDirsResponse.LogDirInfo>> nodes :
                logDirsResult.all().get().entrySet()) {
            for (Map.Entry<String, DescribeLogDirsResponse.LogDirInfo> node : nodes.getValue().entrySet()) {
                for (Map.Entry<TopicPartition, DescribeLogDirsResponse.ReplicaInfo> log :
                        node.getValue().replicaInfos.entrySet()) {
                    System.out.println(log.getKey().topic() + " size is " + log.getValue().size );
                    if (log.getValue().size > 0) {
                        topics.add(log.getKey().topic());
                    }
                }
            }
        }
    }

    private DescribeLogDirsResult getLogDirsResult() throws ExecutionException, InterruptedException {
        DescribeClusterResult dcr = adminClient.describeCluster();
        Collection<Node> dc = dcr.nodes().get();
        System.out.println(dc);
        return adminClient.describeLogDirs(
                dc
                        .stream()
                        .map(Node::id)
                        .collect(Collectors.toList()));
    }
}
