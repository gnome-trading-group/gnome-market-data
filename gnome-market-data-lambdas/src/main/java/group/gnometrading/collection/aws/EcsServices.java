package group.gnometrading.collection.aws;

import group.gnometrading.collection.model.Priority;
import group.gnometrading.collection.model.ShardRecord;
import group.gnometrading.collection.ports.Services;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.regex.Pattern;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import software.amazon.awssdk.services.ecs.EcsClient;
import software.amazon.awssdk.services.ecs.model.CapacityProviderStrategyItem;
import software.amazon.awssdk.services.ecs.model.ContainerDefinition;
import software.amazon.awssdk.services.ecs.model.DesiredStatus;
import software.amazon.awssdk.services.ecs.model.KeyValuePair;
import software.amazon.awssdk.services.ecs.model.Service;
import software.amazon.awssdk.services.ecs.model.TaskDefinition;

/**
 * One ECS service per shard ({@code shard-<id>}, one task) in its region's {@code CollectionCluster-<region>}, on the
 * EC2 capacity provider for its priority: production on-demand, research Spot (decision D5). A production shard held to
 * a zone (its partition's home zone) runs on that zone's on-demand hosts only, {@code collection-ondemand-<region>-<zone
 * ID>}; research shards stay on the region's Spot hosts in any zone. A service can't set its
 * task's environment, so each shard gets its own task definition ({@code shard-<id>}): the region's base definition
 * ({@code collection-shard-<region>}, from CDK) with {@code SHARD_ID} and {@code ORCHESTRATOR_VERSION} added.
 *
 * <p>Deployments stop the old task before starting the new: a shard has its instance to itself, and a version change
 * never updates a service anyway, since a roll hands every stream to a new shard on the new version.
 */
public final class EcsServices implements Services {

    private static final Logger LOGGER = LogManager.getLogger(EcsServices.class);
    private static final String CONTAINER = "ShardContainer";

    private final Function<String, EcsClient> clientFor;

    /** @param clientFor the ECS client for a region (cached by the caller) */
    public EcsServices(final Function<String, EcsClient> clientFor) {
        this.clientFor = clientFor;
    }

    static String cluster(final String region) {
        return "CollectionCluster-" + region;
    }

    static String serviceName(final String shardId) {
        return "shard-" + shardId;
    }

    // What ECS says of a service whose task no host can take.
    private static final Pattern UNPLACED = Pattern.compile("unable to place a task", Pattern.CASE_INSENSITIVE);

    static String capacityProvider(final String region, final Priority priority, final String zoneId) {
        if (priority != Priority.PRODUCTION) {
            return "collection-spot-" + region;
        }
        return zoneId == null ? "collection-ondemand-" + region : "collection-ondemand-" + region + "-" + zoneId;
    }

    @Override
    public void ensure(final ShardRecord shard) {
        final String region = shard.partition().region();
        final EcsClient ecs = ecs(region);
        final List<Service> found = ecs.describeServices(
                        request -> request.cluster(cluster(region)).services(serviceName(shard.shardId())))
                .services();
        if (!found.isEmpty() && "ACTIVE".equals(found.get(0).status())) {
            return;
        }
        final String taskDefinition = registerTaskDefinition(ecs, shard);
        ecs.createService(request -> request.cluster(cluster(region))
                .serviceName(serviceName(shard.shardId()))
                .taskDefinition(taskDefinition)
                .desiredCount(1)
                .capacityProviderStrategy(CapacityProviderStrategyItem.builder()
                        .capacityProvider(capacityProvider(
                                region,
                                shard.partition().priority(),
                                shard.partition().zoneId()))
                        .weight(1)
                        .build())
                .deploymentConfiguration(
                        config -> config.minimumHealthyPercent(0).maximumPercent(100))
                .propagateTags("SERVICE")
                .tags(tag -> tag.key("gnome:shard").value(shard.shardId())));
        LOGGER.info("Started shard {} on {} ({})", shard.shardId(), shard.version(), region);
    }

    @Override
    public void remove(final ShardRecord shard) {
        final String region = shard.partition().region();
        final EcsClient ecs = ecs(region);
        final List<Service> found = ecs.describeServices(
                        request -> request.cluster(cluster(region)).services(serviceName(shard.shardId())))
                .services();
        if (found.isEmpty() || !"ACTIVE".equals(found.get(0).status())) {
            return;
        }
        // force: deletes it with its task still running, which ECS then stops (SIGTERM, then the stop timeout).
        ecs.deleteService(request -> request.cluster(cluster(region))
                .service(serviceName(shard.shardId()))
                .force(true));
        // Each shard registers its own family; without this they would pile up, one per shard ever run.
        ecs.listTaskDefinitionsPaginator(request -> request.familyPrefix(serviceName(shard.shardId())))
                .taskDefinitionArns()
                .forEach(arn -> ecs.deregisterTaskDefinition(request -> request.taskDefinition(arn)));
        LOGGER.info("Deleted shard {} ({})", shard.shardId(), region);
    }

    @Override
    public boolean unplaced(final ShardRecord shard) {
        final String region = shard.partition().region();
        final List<Service> found = ecs(region)
                .describeServices(request -> request.cluster(cluster(region)).services(serviceName(shard.shardId())))
                .services();
        if (found.isEmpty() || !"ACTIVE".equals(found.get(0).status())) {
            return false;
        }
        final Service service = found.get(0);
        if (service.desiredCount() == 0 || service.runningCount() > 0) {
            return false;
        }
        if (!service.events().isEmpty()
                && UNPLACED.matcher(service.events().get(0).message()).find()) {
            return true;
        }
        // The usual sign on a capacity provider: ECS holds the task PROVISIONING until its group launches a host.
        final EcsClient ecs = ecs(region);
        final List<String> tasks = ecs.listTasks(request -> request.cluster(cluster(region))
                        .serviceName(serviceName(shard.shardId()))
                        .desiredStatus(DesiredStatus.RUNNING))
                .taskArns();
        if (tasks.isEmpty()) {
            return false;
        }
        return ecs.describeTasks(request -> request.cluster(cluster(region)).tasks(tasks)).tasks().stream()
                .anyMatch(task -> "PROVISIONING".equals(task.lastStatus()));
    }

    private String registerTaskDefinition(final EcsClient ecs, final ShardRecord shard) {
        final String region = shard.partition().region();
        final TaskDefinition base = ecs.describeTaskDefinition(
                        request -> request.taskDefinition("collection-shard-" + region))
                .taskDefinition();
        final List<ContainerDefinition> containers = new ArrayList<>();
        for (ContainerDefinition container : base.containerDefinitions()) {
            if (!CONTAINER.equals(container.name())) {
                containers.add(container);
                continue;
            }
            final List<KeyValuePair> environment = new ArrayList<>(container.environment());
            environment.add(KeyValuePair.builder()
                    .name("SHARD_ID")
                    .value(shard.shardId())
                    .build());
            environment.add(KeyValuePair.builder()
                    .name("ORCHESTRATOR_VERSION")
                    .value(shard.version())
                    .build());
            containers.add(container.toBuilder().environment(environment).build());
        }
        return ecs.registerTaskDefinition(request -> request.family(serviceName(shard.shardId()))
                        .taskRoleArn(base.taskRoleArn())
                        .executionRoleArn(base.executionRoleArn())
                        .networkMode(base.networkMode())
                        .requiresCompatibilities(base.requiresCompatibilities())
                        .memory(base.memory())
                        .cpu(base.cpu())
                        // The container's mount points name the base's volumes, so a copy without them is refused.
                        .volumes(base.volumes())
                        .containerDefinitions(containers))
                .taskDefinition()
                .taskDefinitionArn();
    }

    private EcsClient ecs(final String region) {
        return clientFor.apply(region);
    }
}
