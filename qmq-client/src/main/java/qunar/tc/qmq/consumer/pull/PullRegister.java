/*
 * Copyright 2018 Qunar, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package qunar.tc.qmq.consumer.pull;

import com.google.common.base.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.broker.BrokerService;
import qunar.tc.qmq.broker.impl.BrokerServiceImpl;
import qunar.tc.qmq.common.ClientType;
import qunar.tc.qmq.common.EnvProvider;
import qunar.tc.qmq.common.MapKeyBuilder;
import qunar.tc.qmq.common.StatusSource;
import qunar.tc.qmq.concurrent.NamedThreadFactory;
import qunar.tc.qmq.config.OrderedMessageManager;
import qunar.tc.qmq.consumer.exception.DuplicateListenerException;
import qunar.tc.qmq.consumer.register.ConsumerRegister;
import qunar.tc.qmq.consumer.register.RegistParam;
import qunar.tc.qmq.meta.PartitionAllocation;
import qunar.tc.qmq.metainfoclient.ConsumerStateChangedListener;
import qunar.tc.qmq.metainfoclient.DefaultMetaInfoService;
import qunar.tc.qmq.metainfoclient.MetaInfoClient;
import qunar.tc.qmq.producer.OrderedMessageUtils;
import qunar.tc.qmq.protocol.MetaInfoResponse;
import qunar.tc.qmq.protocol.consumer.SubEnvIsolationPullFilter;
import qunar.tc.qmq.utils.RetrySubjectUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static qunar.tc.qmq.common.StatusSource.*;

/**
 * @author yiqun.fan create on 17-8-17.
 */
public class PullRegister implements ConsumerRegister, ConsumerStateChangedListener {
    private static final Logger LOG = LoggerFactory.getLogger(PullRegister.class);

    private volatile Boolean isOnline = false;

    private final Map<String, PullEntry> pullEntryMap = new HashMap<>();

    private final Map<String, DefaultPullConsumer> pullConsumerMap = new HashMap<>();

    private final ExecutorService pullExecutor = Executors.newCachedThreadPool(new NamedThreadFactory("qmq-pull"));

    private final DefaultMetaInfoService metaInfoService;
    private final OrderedMessageManager orderedMessageManager;
    private final BrokerService brokerService;
    private final PullService pullService;
    private final AckService ackService;

    private String clientId;
    private String appCode;
    private int destroyWaitInSeconds;

    private EnvProvider envProvider;

    public PullRegister(String metaServer, OrderedMessageManager orderedMessageManager) {
        this.metaInfoService = new DefaultMetaInfoService(metaServer);
        this.orderedMessageManager = orderedMessageManager;
        this.brokerService = new BrokerServiceImpl(metaInfoService);
        this.pullService = new PullService();
        this.ackService = new AckService(this.brokerService);
    }

    public void init() {
        this.metaInfoService.setClientId(clientId);
        this.metaInfoService.init();

        this.ackService.setDestroyWaitInSeconds(destroyWaitInSeconds);
        this.ackService.setClientId(clientId);
        this.metaInfoService.setConsumerStateChangedListener(this);

        this.brokerService.setAppCode(appCode);
    }

    private class PullEntryCreator implements MetaInfoClient.ResponseSubscriber {

        private String subject;
        private String group;
        private RegistParam registParam;

        public PullEntryCreator(String subject, String group, RegistParam registParam) {
            this.subject = subject;
            this.group = group;
            this.registParam = registParam;
        }

        @Override
        public void onResponse(MetaInfoResponse response) {
            Set<Integer> orderedPhysicalPartitions = getOrderedPhysicalPartitions(subject, group, clientId);

            if (orderedPhysicalPartitions != null) {
                for (Integer partition : orderedPhysicalPartitions) {
                    String orderedSubject = OrderedMessageUtils.getOrderedMessageSubject(subject, partition);
                    registerPullEntry(orderedSubject, group, registParam);
                }
            } else {
                registerPullEntry(subject, group, registParam);
            }
        }
    }

    @Override
    public synchronized void register(String subject, String group, RegistParam param) {
        // TODO(zhenwei.liu) 想想 PullConsumer 是否需要重写 注册/心跳 逻辑
        // TODO(zhenwei.liu) 改一下 Producer 的心跳注册逻辑
        // TODO(zhewnei.liu) 如果 producer 发消息是发现 brokerService 信息为空, 需要发消息失败
        // 首先向 meta server 注册, 只有注册成功才往下一步走
        metaInfoService.registerHeartbeat(subject, group, ClientType.CONSUMER, appCode);
        metaInfoService.registerResponseSubscriber(new PullEntryCreator(subject, group, param));
    }

    private synchronized void registerPullEntry(String subject, String group, RegistParam param) {
        String env;
        String subEnv;
        if (envProvider != null && !Strings.isNullOrEmpty(env = envProvider.env(subject))) {
            subEnv = envProvider.subEnv(env);
            final String realGroup = toSubEnvIsolationGroup(group, env, subEnv);
            LOG.info("enable subenv isolation for {}/{}, rename consumer group to {}", subject, group, realGroup);
            group = realGroup;
            param.addFilter(new SubEnvIsolationPullFilter(env, subEnv));
        }

        registerPullEntry(subject, group, param, new AlwaysPullStrategy());
        if (RetrySubjectUtils.isDeadRetrySubject(subject)) return;
        registerPullEntry(RetrySubjectUtils.buildRetrySubject(subject, group), group, param, new WeightPullStrategy());

    }

    private Set<Integer> getOrderedPhysicalPartitions(String subject, String group, String clientId) {
        // 顺序消息拆分主题
        PartitionAllocation partitionAllocation = orderedMessageManager.getPartitionAllocation(subject, group);
        if (partitionAllocation != null) {
            return partitionAllocation.getAllocationDetail().getClientId2PhysicalPartitions().get(clientId);
        }
        return null;
    }

    private String toSubEnvIsolationGroup(final String originGroup, final String env, final String subEnv) {
        return originGroup + "_" + env + "_" + subEnv;
    }

    private void registerPullEntry(String subject, String group, RegistParam param, PullStrategy pullStrategy) {
        final String subscribeKey = MapKeyBuilder.buildSubscribeKey(subject, group);
        PullEntry pullEntry = pullEntryMap.get(subscribeKey);
        if (pullEntry == PullEntry.EMPTY_PULL_ENTRY) {
            throw new DuplicateListenerException(subscribeKey);
        }
        if (pullEntry == null) {
            pullEntry = createAndSubmitPullEntry(subject, group, param, pullStrategy);
        }
        if (isOnline) {
            pullEntry.online(param.getActionSrc());
        } else {
            pullEntry.offline(param.getActionSrc());
        }
    }

    private PullEntry createAndSubmitPullEntry(String subject, String group, RegistParam param, PullStrategy pullStrategy) {
        PushConsumerImpl pushConsumer = new PushConsumerImpl(subject, group, param);
        PullEntry pullEntry = new PullEntry(pushConsumer, pullService, ackService, metaInfoService, brokerService, pullStrategy);
        pullEntryMap.put(MapKeyBuilder.buildSubscribeKey(subject, group), pullEntry);
        pullExecutor.submit(pullEntry);
        return pullEntry;
    }

    DefaultPullConsumer createDefaultPullConsumer(String subject, String group, boolean isBroadcast) {
        DefaultPullConsumer pullConsumer = new DefaultPullConsumer(subject, group, isBroadcast, clientId, pullService, ackService, brokerService);
        registerDefaultPullConsumer(pullConsumer);
        return pullConsumer;
    }

    private synchronized void registerDefaultPullConsumer(DefaultPullConsumer pullConsumer) {
        final String subscribeKey = MapKeyBuilder.buildSubscribeKey(pullConsumer.subject(), pullConsumer.group());
        if (pullEntryMap.containsKey(subscribeKey)) {
            throw new DuplicateListenerException(subscribeKey);
        }
        pullEntryMap.put(subscribeKey, PullEntry.EMPTY_PULL_ENTRY);
        pullConsumerMap.put(subscribeKey, pullConsumer);
        pullExecutor.submit(pullConsumer);
    }

    @Override
    public void unregister(String subject, String group) {
        changeOnOffline(subject, group, false, CODE);
    }

    @Override
    public void online(String subject, String group) {
        changeOnOffline(subject, group, true, OPS);
    }

    @Override
    public void offline(String subject, String group) {
        changeOnOffline(subject, group, false, OPS);
    }

    private synchronized void changeOnOffline(String subject, String group, boolean isOnline, StatusSource src) {
        final String realSubject = RetrySubjectUtils.getRealSubject(subject);
        final String retrySubject = RetrySubjectUtils.buildRetrySubject(realSubject, group);

        final String key = MapKeyBuilder.buildSubscribeKey(realSubject, group);
        final PullEntry pullEntry = pullEntryMap.get(key);
        changeOnOffline(pullEntry, isOnline, src);

        final PullEntry retryPullEntry = pullEntryMap.get(MapKeyBuilder.buildSubscribeKey(retrySubject, group));
        changeOnOffline(retryPullEntry, isOnline, src);

        final DefaultPullConsumer pullConsumer = pullConsumerMap.get(key);
        if (pullConsumer == null) return;

        if (isOnline) {
            pullConsumer.online(src);
        } else {
            pullConsumer.offline(src);
        }
    }

    private void changeOnOffline(PullEntry pullEntry, boolean isOnline, StatusSource src) {
        if (pullEntry == null) return;

        if (isOnline) {
            pullEntry.online(src);
        } else {
            pullEntry.offline(src);
        }
    }

    @Override
    public synchronized void setAutoOnline(boolean autoOnline) {
        if (autoOnline) {
            online();
        } else {
            offline();
        }
        isOnline = autoOnline;
    }

    public synchronized boolean offline() {
        isOnline = false;
        for (PullEntry pullEntry : pullEntryMap.values()) {
            pullEntry.offline(HEALTHCHECKER);
        }
        for (DefaultPullConsumer pullConsumer : pullConsumerMap.values()) {
            pullConsumer.offline(HEALTHCHECKER);
        }
        ackService.tryCleanAck();
        return true;
    }

    public synchronized boolean online() {
        isOnline = true;
        for (PullEntry pullEntry : pullEntryMap.values()) {
            pullEntry.online(HEALTHCHECKER);
        }
        for (DefaultPullConsumer pullConsumer : pullConsumerMap.values()) {
            pullConsumer.online(HEALTHCHECKER);
        }
        return true;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    public void setEnvProvider(EnvProvider envProvider) {
        this.envProvider = envProvider;
    }

    public void setAppCode(String appCode) {
        this.appCode = appCode;
    }

    @Override
    public synchronized void destroy() {
        for (PullEntry pullEntry : pullEntryMap.values()) {
            pullEntry.destroy();
        }
        ackService.destroy();
    }

    public void setDestroyWaitInSeconds(int destroyWaitInSeconds) {
        this.destroyWaitInSeconds = destroyWaitInSeconds;
    }
}
