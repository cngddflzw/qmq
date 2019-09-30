package qunar.tc.qmq.metainfoclient;

import qunar.tc.qmq.StatusSource;
import qunar.tc.qmq.broker.impl.SwitchWaiter;

/**
 * @author zhenwei.liu
 * @since 2019-08-29
 */
public interface ConsumerOnlineStateManager extends MetaInfoClient.ResponseSubscriber {

    void onlineHealthCheck();

    void offlineHealthCheck();

    void online(String subject, String consumerGroup, StatusSource statusSource);

    void offline(String subject, String consumerGroup, StatusSource statusSource);

    boolean isOnline(String subject, String consumerGroup);

    SwitchWaiter registerConsumer(String appCode, String subject, String consumerGroup, String clientId, boolean isBroadcast, boolean isOrdered, MetaInfoService metaInfoService, Runnable offlineCallback);

    SwitchWaiter getSwitchWaiter(String subject, String consumerGroup);
}
