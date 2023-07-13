package kcleaner.delete;

import kcleaner.config.Configuration;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;

import java.util.Set;

@Slf4j
public class TopicDeleterImpl implements TopicDeleter {
    private final AdminClient adminClient;
    private final Configuration configuration;

    public TopicDeleterImpl(AdminClient adminClient, Configuration configuration) {
        this.adminClient = adminClient;
        this.configuration = configuration;
    }

    @Override
    public void delete(Set<String> topicNames) {

    }
}
