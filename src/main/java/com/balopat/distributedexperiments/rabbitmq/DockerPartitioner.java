package com.balopat.distributedexperiments.rabbitmq;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.core.DefaultDockerClientConfig;
import com.github.dockerjava.core.DockerClientBuilder;
import com.github.dockerjava.core.DockerClientConfig;

/**
 * Created by balopat on 1/4/17.
 */
public class DockerPartitioner {

    private DockerClientConfig config;
    private DockerClient docker;
    private RabbitMQClusterManager clusterManager;

    public DockerPartitioner(RabbitMQClusterManager clusterManager) {
        this.clusterManager = clusterManager;
        config = DefaultDockerClientConfig.createDefaultConfigBuilder()
                .build();
        docker = DockerClientBuilder.getInstance(config).build();
    }


    public void healNetwork(String container) {
        on(container).executeCommand("iptables", "-F");
        on(container).executeCommand("iptables", "-L");
    }

    protected RabbitContainer on(String container) {
        return clusterManager.get(container);
    }

    public void partitionAway(String container, String container2, String container3) {
        on(container).executeCommand("iptables", "-A", "INPUT", "-s", ipOf(container2), "-j", "DROP");
        on(container).executeCommand("iptables", "-A", "INPUT", "-s", ipOf(container3), "-j", "DROP");
        on(container).executeCommand("iptables", "-L");
    }

    private String ipOf(String containerName) {
        return clusterManager.ipOf(containerName);
    }





}
