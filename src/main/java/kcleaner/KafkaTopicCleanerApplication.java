package kcleaner;

import kcleaner.find.UnusedTopicsFinder;
import kcleaner.find.UnusedTopicsFinderImpl;
import kcleaner.topic.define.ConsumedTopic;
import kcleaner.topic.define.InternalTopic;
import kcleaner.topic.define.NonEmptyTopic;
import kcleaner.topic.define.ReservedTopic;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.KafkaAdminClient;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static java.util.Arrays.asList;

@Slf4j
public class KafkaTopicCleanerApplication {

    public static void main(String[] args) throws ExecutionException, InterruptedException, IOException {
        final Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka-1i-us-east-1.egdp-prod.aws.away.black:9093");
        properties.load(new FileInputStream("/Users/anamohan/.egsp/prod/ssl.properties"));

        final AdminClient adminClient = KafkaAdminClient.create(properties);
        final Set<String> allTopicNames = adminClient.listTopics().names().get();

        final ReservedTopic consumedTopic = new ConsumedTopic(adminClient);
        final ReservedTopic internalTopic = new InternalTopic(allTopicNames);
        final ReservedTopic nonEmptyTopic = new NonEmptyTopic(adminClient);

        System.out.println("allTopicNames size: "+allTopicNames.size());
        System.out.println("consumedTopic size: "+consumedTopic.getNames().size());
        System.out.println("internalTopic size: "+ internalTopic.getNames().size());
        System.out.println("nonEmptyTopic size: "+ nonEmptyTopic.getNames().size());

        final UnusedTopicsFinder unusedTopicsFinder = new UnusedTopicsFinderImpl(allTopicNames,
                new HashSet<>(asList(consumedTopic, internalTopic, nonEmptyTopic)));

        final Set<String> unusedTopics = unusedTopicsFinder.getUnusedTopics();
        System.out.println("unusedTopics size: "+ unusedTopics.size());

        System.out.println("########################");
        System.out.println("consumedTopic: "+consumedTopic.getNames());
        System.out.println("########################");
        System.out.println("internalTopic: "+ internalTopic.getNames());
        System.out.println("########################");
        System.out.println("nonEmptyTopic: "+ nonEmptyTopic.getNames());
        System.out.println("########################");
        System.out.println("unusedTopics: "+ unusedTopics);
        System.out.println("########################");
        adminClient.close();
        log.info("Client closed");
    }
}
