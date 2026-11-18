package group.gnometrading.collection.aws;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import group.gnometrading.collection.model.Partition;
import group.gnometrading.collection.model.Priority;
import group.gnometrading.collection.model.ShardRecord;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.ecs.EcsClient;
import software.amazon.awssdk.services.ecs.model.DescribeServicesResponse;
import software.amazon.awssdk.services.ecs.model.DescribeTasksResponse;
import software.amazon.awssdk.services.ecs.model.ListTasksResponse;
import software.amazon.awssdk.services.ecs.model.Service;
import software.amazon.awssdk.services.ecs.model.ServiceEvent;
import software.amazon.awssdk.services.ecs.model.Task;

class EcsServicesTest {

    private static final String UNPLACED = "(service shard-use2-1) was unable to place a task because no container"
            + " instance met all of its requirements.";

    @Test
    void aProductionShardHeldToAZoneRunsOnThatZonesOnDemandHostsAndResearchOnTheRegionsSpot() {
        assertEquals(
                "collection-ondemand-us-east-2-use2-az2",
                EcsServices.capacityProvider("us-east-2", Priority.PRODUCTION, "use2-az2"));
        assertEquals(
                "collection-ondemand-us-east-2", EcsServices.capacityProvider("us-east-2", Priority.PRODUCTION, null));
        assertEquals("collection-spot-us-east-2", EcsServices.capacityProvider("us-east-2", Priority.RESEARCH, null));
        assertEquals(
                "collection-spot-us-east-2", EcsServices.capacityProvider("us-east-2", Priority.RESEARCH, "use2-az2"));
    }

    @Test
    void aShardIsUnplacedWhenItWantsATaskNoneRunsAndEcsSaysItCantPlaceIt() {
        assertTrue(unplaced(service(1, 0, UNPLACED), null));
        assertFalse(unplaced(service(1, 1, UNPLACED), null), "a task runs");
        assertFalse(unplaced(service(0, 0, UNPLACED), null), "nothing wanted");
        assertFalse(unplaced(null, null), "no service");
    }

    @Test
    void aShardIsUnplacedWhileItsTaskWaitsProvisioningForAHost() {
        final Service started = service(1, 0, "(service shard-use2-1) has started 1 tasks");
        assertTrue(unplaced(started, "PROVISIONING"));
        assertFalse(unplaced(started, "PENDING"), "it has a host, and is pulling its image");
        assertFalse(unplaced(started, null), "no task yet");
    }

    private static Service service(final int desired, final int running, final String event) {
        return Service.builder()
                .status("ACTIVE")
                .desiredCount(desired)
                .runningCount(running)
                .events(ServiceEvent.builder().message(event).build())
                .build();
    }

    @SuppressWarnings("unchecked")
    private static boolean unplaced(final Service service, final String taskStatus) {
        final EcsClient ecs = mock(EcsClient.class);
        when(ecs.describeServices(any(Consumer.class)))
                .thenReturn(DescribeServicesResponse.builder()
                        .services(service == null ? List.of() : List.of(service))
                        .build());
        when(ecs.listTasks(any(Consumer.class)))
                .thenReturn(ListTasksResponse.builder()
                        .taskArns(taskStatus == null ? List.of() : List.of("arn:task/1"))
                        .build());
        when(ecs.describeTasks(any(Consumer.class)))
                .thenReturn(DescribeTasksResponse.builder()
                        .tasks(Task.builder().lastStatus(taskStatus).build())
                        .build());
        final ShardRecord shard = new ShardRecord(
                "use2-1",
                new Partition("us-east-2", "VENUE", Priority.PRODUCTION, "standard", "use2-az2"),
                1,
                List.of(1),
                List.of(),
                Map.of(),
                Map.of(),
                List.of(1),
                "1.23.0",
                ShardRecord.State.ACTIVE,
                100,
                0,
                0,
                0,
                0);
        return new EcsServices(region -> ecs).unplaced(shard);
    }
}
