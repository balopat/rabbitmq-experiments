package com.balopat.distributedexperiments.rabbitmq;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.ExecCreateCmdResponse;
import com.github.dockerjava.api.model.Container;
import com.github.dockerjava.api.model.Frame;
import com.github.dockerjava.core.command.ExecStartResultCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class RabbitContainer {
    private static final Logger LOG = LoggerFactory.getLogger(RabbitContainer.class);
    String name;
        String ipAddress;
        String containerId;
        Container container;
    private DockerClient docker;

    public RabbitContainer(String name, String ipAddress, String containerId, Container container, DockerClient docker) {
            this.name = name;
            this.ipAddress = ipAddress;
            this.containerId = containerId;
            this.container = container;
        this.docker = docker;
    }


        public String executeCommand(String... command) {
            StringBuffer commandOutput = new StringBuffer();
            ExecCreateCmdResponse response = docker
                    .execCreateCmd(container.getId())
                    .withCmd(command)
                    .withAttachStdout(true)
                    .withAttachStderr(true)
                    .exec();

            try {
                docker.execStartCmd(response.getId())
                        .withDetach(false)
                        .withTty(true)
                        .exec(new ExecStartResultCallback() {
                            @Override
                            public void onNext(Frame frame) {
                                String frameToString = new String(frame.getPayload());
                                LOG.info(frameToString);
                                commandOutput.append(frameToString);
                            }
                        }).awaitCompletion().close();
            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
            }
            return commandOutput.toString();
        }


        @Override
        public String toString() {
            return "RabbitContainer{" +
                    "name='" + name + '\'' +
                    ", ipAddress='" + ipAddress + '\'' +
                    ", containerId='" + containerId + '\'' +
                    '}';
        }
    }