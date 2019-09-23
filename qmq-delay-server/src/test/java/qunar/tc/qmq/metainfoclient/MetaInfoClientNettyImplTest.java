package qunar.tc.qmq.metainfoclient;

import org.junit.Test;
import qunar.tc.qmq.ClientType;
import qunar.tc.qmq.base.ClientRequestType;
import qunar.tc.qmq.meta.MetaServerLocator;
import qunar.tc.qmq.protocol.consumer.MetaInfoRequest;

/**
 * @author zhenwei.liu
 * @since 2019-09-23
 */
public class MetaInfoClientNettyImplTest {

    @Test
    public void testSendRequest() throws Exception {
        MetaInfoClientNettyImpl client = MetaInfoClientNettyImpl.getClient(new MetaServerLocator("http://localhost:8080/meta/address"));
        client.sendMetaInfoRequest(new MetaInfoRequest(
                "test",
                "test",
                ClientType.PRODUCER.getCode(),
                "test",
                "test",
                ClientRequestType.ONLINE,
                false,
                false
                ));
        Thread.sleep(60000);
    }
}
