package com.balopat.distributedexperiments.rabbitmq;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.balopat.distributedexperiments.rabbitmq.RabbitMQClusterManager.*;

/**
 * Created by balopat on 1/10/17.
 */
public class ClusterStateValidator {

    private static Logger LOG = LoggerFactory.getLogger(ClusterStateValidator.class);

    private RabbitMQClusterManager clusterManager;
    private List<ClusterStateFromNode> clusterStateAssertionsByNode = new ArrayList<>();

    public ClusterStateValidator(RabbitMQClusterManager clusterManager) {
        this.clusterManager = clusterManager;
    }

    public ClusterStateFromNode from(String nodeName) {
        ClusterStateFromNode clusterStateFromNode = new ClusterStateFromNode(nodeName, this);
        this.clusterStateAssertionsByNode.add(clusterStateFromNode);
        return clusterStateFromNode;
    }

    public boolean validate() {
        boolean result = true;
        for (ClusterStateFromNode fromNode : clusterStateAssertionsByNode) {
            String output = clusterManager.get(fromNode.fromNode).executeCommand("rst");

            boolean[] actualClusterStateFromNode = parse(output);
            if (!Arrays.equals(actualClusterStateFromNode, fromNode.expectedClusterState)) {
                LOG.warn("cluster state is not matching expected state, from node \n exp: " + fromNode + "\n act: "
                        + Arrays.toString(actualClusterStateFromNode));
                result = false;
            }
        }

        return result;
    }

    protected boolean[] parse(String output) {
        if (output.split("\n").length > 3) {
            String firstLineOfOutput = output.split("\n")[3];
            return new boolean[]{
                    firstLineOfOutput.contains(RABBIT1),
                    firstLineOfOutput.contains(RABBIT2),
                    firstLineOfOutput.contains(RABBIT3)
            };
        } else {
            return new boolean[]{false, false, false};
        }
    }

    public class ClusterStateFromNode {

        private final String fromNode;
        private final ClusterStateValidator clusterStateValidatorBuilder;
        private boolean[] expectedClusterState = new boolean[]{true, true, true};

        public ClusterStateFromNode(String fromNode, ClusterStateValidator clusterStateValidatorBuilder) {
            this.fromNode = fromNode;
            this.clusterStateValidatorBuilder = clusterStateValidatorBuilder;
        }

        public ClusterStateValidator clusteredNodesAre(boolean rabbit1, boolean rabbit2, boolean rabbit3) {
            expectedClusterState = new boolean[]{rabbit1, rabbit2, rabbit3};
            return clusterStateValidatorBuilder;
        }

        @Override
        public String toString() {
            return "fromNode='" + fromNode + '\'' +
                    ", expected=" + Arrays.toString(expectedClusterState);
        }
    }
}
