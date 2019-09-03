package qunar.tc.qmq.meta.order;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.meta.Partition;
import qunar.tc.qmq.PartitionAllocation;
import qunar.tc.qmq.meta.PartitionMapping;
import qunar.tc.qmq.meta.PartitionSet;
import qunar.tc.qmq.meta.cache.CachedMetaInfoManager;
import qunar.tc.qmq.meta.model.ClientMetaInfo;

import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * @author zhenwei.liu
 * @since 2019-08-28
 */
public class PartitionAllocationTask {

    private static final Logger logger = LoggerFactory.getLogger(PartitionAllocationTask.class);

    private ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor(
            new ThreadFactoryBuilder().setNameFormat("partition-allocation-thread-%s").build()
    );

    private OrderedMessageService orderedMessageService = DefaultOrderedMessageService.getInstance();
    private CachedMetaInfoManager cachedMetaInfoManager;

    public PartitionAllocationTask(CachedMetaInfoManager cachedMetaInfoManager) {
        this.cachedMetaInfoManager = cachedMetaInfoManager;
    }

    public void start() {
        executor.scheduleWithFixedDelay(() -> {
            try {
                updatePartitionAllocation();
            } catch (Throwable t) {
                logger.error("检查顺序消息分配失败 ", t);
            }
        }, 0, 5, TimeUnit.SECONDS);
    }

    private void updatePartitionAllocation() {
        // 当前 client 在线列表
        List<ClientMetaInfo> onlineConsumers = orderedMessageService.getOnlineOrderedConsumers();
        // 当前分配情况
        Map<String, List<ClientMetaInfo>> onlineConsumerMap = Maps.newHashMap();

        for (ClientMetaInfo consumer : onlineConsumers) {
            String key = consumer.getSubject() + ":" + consumer.getConsumerGroup();
            List<ClientMetaInfo> consumerGroup = onlineConsumerMap.computeIfAbsent(key, k -> Lists.newArrayList());
            consumerGroup.add(consumer);
        }

        for (Map.Entry<String, List<ClientMetaInfo>> entry : onlineConsumerMap.entrySet()) {
            List<ClientMetaInfo> groupOnlineConsumers = entry.getValue();
            String subject = groupOnlineConsumers.get(0).getSubject();
            String consumerGroup = groupOnlineConsumers.get(0).getConsumerGroup();
            reallocation(subject, consumerGroup, groupOnlineConsumers);
        }
    }

    public void reallocation(String subject, String consumerGroup) {
        List<ClientMetaInfo> onlineConsumers = orderedMessageService.getOnlineOrderedConsumers(subject, consumerGroup);
        reallocation(subject, consumerGroup, onlineConsumers);
    }

    public void reallocation(String subject, String consumerGroup, List<ClientMetaInfo> groupOnlineConsumers) {
        String key = subject + ":" + consumerGroup;
        Set<String> groupOnlineConsumerIds = groupOnlineConsumers.stream().map(ClientMetaInfo::getClientId).collect(Collectors.toSet());

        Map<String, PartitionAllocation> allocationMap = cachedMetaInfoManager.getPartitionAllocations();
        PartitionAllocation allocation = allocationMap.get(key);
        if (allocation != null) {
            PartitionAllocation.AllocationDetail allocationDetail = allocation.getAllocationDetail();
            Set<String> allocationClientIds = allocationDetail.getClientId2PhysicalPartitions().keySet();

            if (!Objects.equals(allocationClientIds, groupOnlineConsumerIds)) {
                // 如果当前在线列表与当前分配情况发生变更, 触发重分配
                reallocation(subject, consumerGroup, Lists.newArrayList(groupOnlineConsumerIds), allocation.getVersion());
            }
        } else {
            // 首次分配
            reallocation(subject, consumerGroup, Lists.newArrayList(groupOnlineConsumerIds), -1);
        }
    }

    private void reallocation(String subject, String consumerGroup, List<String> groupOnlineConsumerIds, int oldVersion) {
        PartitionMapping partitionMapping = cachedMetaInfoManager.getPartitionMapping(subject);
        PartitionSet partitionSet = new PartitionSet();
        partitionSet.setSubject(subject);
        Collection<Partition> partitions = partitionMapping.getLogical2PhysicalPartition().asMapOfRanges().values();
        partitionSet.setPhysicalPartitions(partitions.stream().map(Partition::getPhysicalPartition).collect(Collectors.toSet()));
        partitionSet.setVersion(partitionMapping.getVersion());

        // 重新分配
        PartitionAllocation newAllocation = orderedMessageService.allocatePartitions(partitionSet, groupOnlineConsumerIds, consumerGroup);

        // 乐观锁更新
        if (orderedMessageService.updatePartitionAllocation(newAllocation, oldVersion)) {
            // TODO(zhenwei.liu) 重分配成功后给 client 发个拉取通知?
            logger.info("分区重分配成功 subject {} group {} oldVersion {} detail {}",
                    subject, consumerGroup, oldVersion,
                    Arrays.toString(newAllocation.getAllocationDetail().getClientId2PhysicalPartitions().entrySet().toArray()));
        } else {
            logger.warn("分区重分配失败 subject {} group {} oldVersion {}",
                    subject, consumerGroup, oldVersion);
        }
    }
}
