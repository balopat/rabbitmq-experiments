package com.balopat.distributedexperiments.rabbitmq;

import org.junit.Test;

import static com.balopat.distributedexperiments.rabbitmq.RabbitMQClusterManager.RABBIT1;
import static com.balopat.distributedexperiments.rabbitmq.RabbitMQClusterManager.RABBIT2;
import static com.balopat.distributedexperiments.rabbitmq.RabbitMQClusterManager.RABBIT3;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.*;

/**
 * Created by balopat on 1/10/17.
 */
public class ClusterStateValidatorIntegrationTest {


    @Test
    public void testSomething() {
        assertThat((new ClusterStateValidator(null)).parse("\nCluster status of node rabbit@rabbit2 ...\n[{nodes,[{disc,[rabbit@rabbit1,rabbit@rabbit2,rabbit@rabbit3]}]},\n" +
                " {running_nodes,[rabbit@rabbit3,rabbit@rabbit1]},\n" +
                " {cluster_name,<<\"rabbit@rabbit1\">>},\n" +
                " {partitions,[]}]"), is(new boolean[]{true, false, true}));
    }

    @Test
    public void checkAssumptionsAboutRabbitMQClusterStatusOutput() throws Exception {
        RabbitMQClusterManager clusterManager = new RabbitMQClusterManager();
        clusterManager.bringUpCluster();
        boolean clusterStateIsValid = clusterManager.assertClusteringState()
                .from(RABBIT1).clusteredNodesAre(true, true, true)
                .from(RABBIT2).clusteredNodesAre(true, true, true)
                .from(RABBIT3).clusteredNodesAre(true, true, true)
                .validate();

        if (!clusterStateIsValid) {
            throw new RuntimeException("Cluster is not in right state!");
        }
    }
}