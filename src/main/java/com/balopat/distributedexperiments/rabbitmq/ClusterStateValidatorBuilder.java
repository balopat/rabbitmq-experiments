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
public class ClusterStateValidatorBuilder {

    private static Logger LOG = LoggerFactory.getLogger(ClusterStateValidatorBuilder.class);

    private RabbitMQClusterManager clusterManager;
    private List<ClusterStateFromNode> clusterStateAssertionsByNode = new ArrayList<>();

    public ClusterStateValidatorBuilder(RabbitMQClusterManager clusterManager) {
        this.clusterManager = clusterManager;
    }

    public ClusterStateFromNode from(String nodeName) {
        ClusterStateFromNode clusterStateFromNode = new ClusterStateFromNode(nodeName, this);
        this.clusterStateAssertionsByNode.add(clusterStateFromNode);
        return clusterStateFromNode;
    }

    public boolean validate() {
        boolean result = true;
        for (ClusterStateFromNode expectedClusterStateFromNode : clusterStateAssertionsByNode) {
            String output = clusterManager.get(expectedClusterStateFromNode.fromNode).executeCommand("rst");
            String firstLineOfOutput = output.split("\n")[0];
            boolean[] actualClusterStateFromNode = parse(firstLineOfOutput);
            if (!actualClusterStateFromNode.equals(expectedClusterStateFromNode)) {
                LOG.warn("cluster state is not matching expected state, from node " + expectedClusterStateFromNode + " it is: \n" + actualClusterStateFromNode);
                result = false;
            }
        }

        return result;
    }

    private boolean[] parse(String firstLineOfOutput) {
        return new boolean[]{
                firstLineOfOutput.contains(RABBIT1),
                firstLineOfOutput.contains(RABBIT2),
                firstLineOfOutput.contains(RABBIT3)
        };
    }

    public class ClusterStateFromNode {

        private final String fromNode;
        private final ClusterStateValidatorBuilder clusterStateValidatorBuilder;
        private boolean[] expectedClusterState = new boolean[]{true, true, true};

        public ClusterStateFromNode(String fromNode, ClusterStateValidatorBuilder clusterStateValidatorBuilder) {
            this.fromNode = fromNode;
            this.clusterStateValidatorBuilder = clusterStateValidatorBuilder;
        }

        public ClusterStateValidatorBuilder clusteredNodesAre(boolean rabbit1, boolean rabbit2, boolean rabbit3) {
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
