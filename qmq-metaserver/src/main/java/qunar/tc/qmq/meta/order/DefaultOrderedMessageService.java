package qunar.tc.qmq.meta.order;

import com.google.common.collect.*;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.transaction.support.TransactionTemplate;
import qunar.tc.qmq.PartitionAllocation;
import qunar.tc.qmq.base.OnOfflineState;
import qunar.tc.qmq.common.ClientType;
import qunar.tc.qmq.jdbc.JdbcTemplateHolder;
import qunar.tc.qmq.meta.Partition;
import qunar.tc.qmq.meta.PartitionMapping;
import qunar.tc.qmq.meta.PartitionSet;
import qunar.tc.qmq.meta.model.ClientMetaInfo;
import qunar.tc.qmq.meta.model.SubjectInfo;
import qunar.tc.qmq.meta.store.*;
import qunar.tc.qmq.meta.store.impl.*;

import java.util.*;
import java.util.stream.Collectors;

import static qunar.tc.qmq.common.OrderedConstants.ORDERED_CLIENT_HEARTBEAT_INTERVAL_SECS;
import static qunar.tc.qmq.common.OrderedConstants.ORDERED_CONSUMER_LOCK_LEASE_SECS;

/**
 * @author zhenwei.liu
 * @since 2019-08-22
 */
public class DefaultOrderedMessageService implements OrderedMessageService {

    private Store store = new DatabaseStore();
    private ClientMetaInfoStore clientMetaInfoStore = new ClientMetaInfoStoreImpl();
    private PartitionStore partitionStore = new PartitionStoreImpl();
    private PartitionSetStore partitionSetStore = new PartitionSetStoreImpl();
    private PartitionAllocationStore partitionAllocationStore = new PartitionAllocationStoreImpl();
    private PartitionAllocationStrategy partitionAllocationStrategy = new AveragePartitionAllocationStrategy();
    private TransactionTemplate transactionTemplate = JdbcTemplateHolder.getTransactionTemplate();
    private RangeMapper rangeMapper = new AverageRangeMapper();
    private ItemMapper itemMapper = new AverageItemMapper();

    private static DefaultOrderedMessageService instance = new DefaultOrderedMessageService();

    public static DefaultOrderedMessageService getInstance() {
        return instance;
    }

    private DefaultOrderedMessageService() {
    }

    @Override
    public void registerOrderedMessage(String subject, int physicalPartitionNum) {

        SubjectInfo subjectInfo = store.getSubjectInfo(subject);
        if (subjectInfo != null) {
            throw new IllegalArgumentException(String.format("subject %s 已存在", subject));
        }

        int logicalPartitionNum = OrderedMessageConfig.getDefaultLogicalPartitionNum();
        int defaultPhysicalPartitionNum = OrderedMessageConfig.getDefaultPhysicalPartitionNum();
        physicalPartitionNum = physicalPartitionNum == 0 ? defaultPhysicalPartitionNum : physicalPartitionNum;
        List<String> brokerGroups = OrderedMessageConfig.getBrokerGroups();

        // 初始化 physical/logical partition 列表
        List<Integer> physicalPartitionList = createIntList(physicalPartitionNum);
        List<Integer> logicalPartitionList = createIntList(logicalPartitionNum);

        // logical => physical mapping
        Map<Integer, List<Integer>> physical2LogicalPartitionMap = rangeMapper.map(physicalPartitionList, logicalPartitionList);

        // physical => brokerGroup mapping
        Map<Integer, String> physical2BrokerGroupMap = itemMapper.map(physicalPartitionList, brokerGroups);

        List<Partition> partitions = Lists.newArrayList();
        for (Map.Entry<Integer, List<Integer>> entry : physical2LogicalPartitionMap.entrySet()) {
            Integer physicalPartition = entry.getKey();
            List<Integer> logicalPartitions = entry.getValue();
            Range<Integer> logicalPartitionRange =
                    Range.closedOpen(logicalPartitions.get(0), logicalPartitions.get(logicalPartitions.size() - 1) + 1);

            Partition partition = new Partition();
            partition.setSubject(subject);
            partition.setLogicalPartition(logicalPartitionRange);
            partition.setPhysicalPartition(physicalPartition);
            partition.setStatus(Partition.Status.RW);
            partition.setBrokerGroup(physical2BrokerGroupMap.get(physicalPartition));

            partitions.add(partition);
        }

        PartitionSet partitionSet = new PartitionSet();
        partitionSet.setSubject(subject);
        partitionSet.setVersion(0);
        partitionSet.setPhysicalPartitions(partitions.stream().map(Partition::getPhysicalPartition).collect(Collectors.toSet()));

        transactionTemplate.execute(transactionStatus -> {
            partitionStore.save(partitions);
            partitionSetStore.save(partitionSet);
            return null;
        });
    }

    @Override
    public PartitionAllocation getActivatedPartitionAllocation(String subject, String group) {
        return partitionAllocationStore.getLatest(subject, group);
    }

    @Override
    public List<PartitionAllocation> getActivatedPartitionAllocations() {
        return partitionAllocationStore.getLatest();
    }

    @Override
    public List<PartitionMapping> getLatestPartitionMappings() {
        List<PartitionMapping> result = Lists.newArrayList();
        List<PartitionSet> latestPartitionSets = partitionSetStore.getLatest();

        Set<Integer> partitionIdSet = Sets.newTreeSet();
        latestPartitionSets.forEach(partitionSet -> partitionIdSet.addAll(partitionSet.getPhysicalPartitions()));
        List<Integer> partitionIdList = Lists.newArrayList(partitionIdSet);
        // subject+id => partition
        Map<String, Partition> partitionMap = Maps.newHashMap();
        int start = 0;
        int step = 100;
        while (start < partitionIdList.size()) {
            int end = start + step;
            if (end > partitionIdList.size()) {
                end = partitionIdList.size();
            }
            List<Integer> subList = partitionIdList.subList(start, end);
            List<Partition> partitionList = partitionStore.getByPartitionIds(subList);
            for (Partition partition : partitionList) {
                partitionMap.put(createPartitionKey(partition), partition);
            }
            start += step;
        }

        for (PartitionSet partitionSet : latestPartitionSets) {
            PartitionMapping partitionMapping = transformPartitionMapping(partitionSet, partitionMap);
            result.add(partitionMapping);
        }

        return result;
    }

    @Override
    public PartitionMapping getLatestPartitionMapping(String subject) {
        PartitionSet partitionSet = partitionSetStore.getLatest(subject);
        List<Partition> partitionList = partitionStore.getByPartitionIds(partitionSet.getPhysicalPartitions());
        Map<String, Partition> partitionMap = partitionList.stream().collect(Collectors.toMap(this::createPartitionKey, p -> p));
        return transformPartitionMapping(partitionSet, partitionMap);
    }

    @Override
    public PartitionSet getLatestPartitionSet(String subject) {
        return partitionSetStore.getLatest(subject);
    }

    private PartitionMapping transformPartitionMapping(PartitionSet partitionSet, Map<String, Partition> partitionMap) {
        PartitionMapping partitionMapping = new PartitionMapping();

        partitionMapping.setSubject(partitionSet.getSubject());
        partitionMapping.setVersion(partitionSet.getVersion());

        Set<Integer> physicalPartitions = partitionSet.getPhysicalPartitions();
        RangeMap<Integer, Partition> logical2PhysicalPartition = TreeRangeMap.create();
        int logicalPartitionNum = 0;
        for (Integer physicalPartition : physicalPartitions) {
            String partitionKey = createPartitionKey(partitionSet.getSubject(), physicalPartition);
            Partition partition = partitionMap.get(partitionKey);
            Range<Integer> logicalPartition = partition.getLogicalPartition();
            logicalPartitionNum += (logicalPartition.upperEndpoint() - logicalPartition.lowerEndpoint());
            logical2PhysicalPartition.put(logicalPartition, partition);
        }
        partitionMapping.setLogical2PhysicalPartition(logical2PhysicalPartition);
        partitionMapping.setLogicalPartitionNum(logicalPartitionNum);

        return partitionMapping;
    }

    @Override
    public PartitionAllocation allocatePartitions(PartitionSet partitionSet, List<String> onlineConsumerList, String consumerGroup) {
        return partitionAllocationStrategy.allocate(partitionSet, onlineConsumerList, consumerGroup);
    }

    @Override
    public boolean updatePartitionAllocation(PartitionAllocation newAllocation, int baseVersion) {
        if (baseVersion == -1) {
            try {
                return partitionAllocationStore.save(newAllocation) > 0;
            } catch (DuplicateKeyException e) {
                // 并发问题, 不更新就好
                return false;
            }
        } else {
            return partitionAllocationStore.update(newAllocation, baseVersion) > 0;
        }
    }

    @Override
    public List<ClientMetaInfo> getOnlineOrderedConsumers() {
        Date updateTime = new Date(System.currentTimeMillis() - ORDERED_CONSUMER_LOCK_LEASE_SECS * 1000);
        return clientMetaInfoStore.queryClientsUpdateAfterDate(ClientType.CONSUMER, OnOfflineState.ONLINE, updateTime);
    }

    @Override
    public List<ClientMetaInfo> getOnlineOrderedConsumers(String subject, String consumerGroup) {
        Date updateTime = new Date(System.currentTimeMillis() - ORDERED_CONSUMER_LOCK_LEASE_SECS * 1000);
        return clientMetaInfoStore.queryClientsUpdateAfterDate(subject, consumerGroup, ClientType.CONSUMER, OnOfflineState.ONLINE, updateTime);
    }

    @Override
    public long getExpiredMills(long startMills) {
        return startMills + ORDERED_CONSUMER_LOCK_LEASE_SECS * 1000;
    }

    private String createPartitionKey(Partition partition) {
        return createPartitionKey(partition.getSubject(), partition.getPhysicalPartition());
    }

    private String createPartitionKey(String subject, int physicalPartition) {
        return subject + ":" + physicalPartition;
    }

    private List<Integer> createIntList(int size) {
        ArrayList<Integer> intList = Lists.newArrayList();
        for (int i = 0; i < size; i++) {
            intList.add(i);
        }
        return intList;
    }
}
