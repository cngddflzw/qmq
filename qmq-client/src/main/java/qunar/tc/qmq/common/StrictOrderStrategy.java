package qunar.tc.qmq.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.ProduceMessage;
import qunar.tc.qmq.base.BaseMessage;
import qunar.tc.qmq.producer.sender.SendMessageExecutor;
import qunar.tc.qmq.consumer.ConsumeMessageExecutor;
import qunar.tc.qmq.consumer.pull.PulledMessage;
import qunar.tc.qmq.producer.sender.SendMessageExecutorManager;

/**
 * @author zhenwei.liu
 * @since 2019-09-10
 */
public class StrictOrderStrategy extends AbstractOrderStrategy {

    private static final Logger logger = LoggerFactory.getLogger(StrictOrderStrategy.class);

    @Override
    public void doOnSendError(ProduceMessage message, SendMessageExecutor currentExecutor, SendMessageExecutorManager sendMessageExecutorManager, Exception e) {
        message.incTries();
    }

    @Override
    public void onConsumeFailed(PulledMessage message, ConsumeMessageExecutor executor, Throwable t) {
        executor.requeueFirst(message);
    }

    @Override
    public void onMessageNotAcked(PulledMessage message, ConsumeMessageExecutor executor) {
        logger.error("消息未 ACK, 将进行重复消费, 请检查程序是否存在 BUG");
        executor.requeueFirst(message);
    }

    @Override
    public boolean isDeadRetry(int nextRetryCount, BaseMessage message) {
        return true;
    }

    @Override
    public String name() {
        return STRICT;
    }
}
