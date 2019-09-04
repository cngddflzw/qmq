package qunar.tc.qmq.producer.sender;

import qunar.tc.qmq.ProduceMessage;
import qunar.tc.qmq.base.BaseMessage;
import qunar.tc.qmq.base.BaseMessage.keys;
import qunar.tc.qmq.common.OrderedMessageUtils;

import java.util.List;

/**
 * @author zhenwei.liu
 * @since 2019-08-20
 */
public class OrderedSendMessagePreHandler implements SendMessagePreHandler {

    @Override
    public void handle(List<ProduceMessage> messages) {
        messages.forEach(message -> {
            BaseMessage baseMessage = (BaseMessage) message.getBase();
            // 消息发到 broker 后, 由 broker 自行修改主题
//            int physicalPartition = baseMessage.getIntProperty(keys.qmq_physicalPartition.name());
//            baseMessage.setSubject(OrderedMessageUtils.getOrderedMessageSubject(baseMessage.getSubject(), physicalPartition));
            baseMessage.removeProperty(keys.qmq_queueSenderType);
            baseMessage.removeProperty(keys.qmq_loadBalanceType);
        });
    }
}
