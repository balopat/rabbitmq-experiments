package com.balopat.distributedexperiments.rabbitmq;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.CreateContainerResponse;
import com.github.dockerjava.api.model.*;
import com.github.dockerjava.core.DefaultDockerClientConfig;
import com.github.dockerjava.core.DockerClientBuilder;
import com.github.dockerjava.core.DockerClientConfig;
import com.github.dockerjava.core.command.LogContainerResultCallback;
import com.google.common.collect.Maps;
import org.apache.http.client.HttpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Created by balopat on 1/6/17.
 */
public class RabbitMQClusterManager {
    private static final Logger LOG = LoggerFactory.getLogger(RabbitMQClusterManager.class);
    private DockerClientConfig config;
    private DockerClient docker;

    public RabbitMQClusterManager() {
        config = DefaultDockerClientConfig.createDefaultConfigBuilder()
                .build();
        docker = DockerClientBuilder.getInstance(config).build();
    }


    public void bringUpCluster() throws InterruptedException, IOException {
        LOG.info("bringing up cluster");
        runRabbitMQClusterManagerContainerWithCmd();

        boolean isClusterUp = false;

        while (!isClusterUp) {
            try {
                HttpURLConnection connection = (HttpURLConnection) new URL("http://localhost:15672/api/aliveness-test/%2F").openConnection();
                String encoded = Base64.getEncoder().encodeToString(("guest:guest").getBytes(StandardCharsets.UTF_8));  //Java 8
                connection.setRequestProperty("Authorization", "Basic " + encoded);
                LOG.info(connection.getResponseCode() + " -> " + connection.getResponseMessage());
                isClusterUp = connection.getResponseCode() / 200 == 1;
            }catch (Exception e) {
                LOG.info("Rabbit cluster is not up yet: " + e + "\n sleeping for 5 seconds...");
                Thread.sleep(5000);
            }
        }

    }

    public void cleanup() throws InterruptedException {
        LOG.info("running cleanup");
        runRabbitMQClusterManagerContainerWithCmd("bash", "./cleanup.sh");
    }

    private void runRabbitMQClusterManagerContainerWithCmd(String... cmd) throws InterruptedException {
        Map<String, String> labels = new HashMap<>();
        labels.put("app","rabbitmq-cluster-manager");
        CreateContainerResponse container =  docker.createContainerCmd("balopat/rabbitmq-cluster-manager:latest")
                .withPrivileged(true)
                .withAttachStdout(true)
                .withAttachStderr(true)
                .withBinds(new Bind("/var/run/docker.sock", new Volume("/var/run/docker.sock"), AccessMode.rw))
                .withLabels(labels)
                .withCmd(cmd)
                .exec();

        docker.startContainerCmd(container.getId())
                .exec();


        docker.logContainerCmd(container.getId()).withStdOut(true).withStdErr(true).withFollowStream(true).exec(new LogContainerResultCallback(){
            @Override
            public void onNext(Frame item) {
                LOG.info(item.toString());
            }
        }).awaitCompletion();

        docker.removeContainerCmd(container.getId()).exec();
    }
}
