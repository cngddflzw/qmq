package qunar.tc.qmq.consumer.pull;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;
import qunar.tc.qmq.CompositePullClient;
import qunar.tc.qmq.PullClient;
import qunar.tc.qmq.PullEntry;
import qunar.tc.qmq.broker.BrokerService;
import qunar.tc.qmq.common.EnvProvider;
import qunar.tc.qmq.consumer.ConsumeMessageExecutorFactory;
import qunar.tc.qmq.meta.ConsumerAllocation;
import qunar.tc.qmq.metainfoclient.ConsumerOnlineStateManager;
import qunar.tc.qmq.metainfoclient.MetaInfoService;
import qunar.tc.qmq.protocol.consumer.ConsumerMetaInfoResponse;
import qunar.tc.qmq.utils.RetryPartitionUtils;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutorService;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mockStatic;
import static qunar.tc.qmq.ClientTestUtils.*;

/**
 * @author zhenwei.liu
 * @since 2019-09-27
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(ConsumeMessageExecutorFactory.class)
public class PullEntryManagerTest {

    @Mock
    private ConsumerOnlineStateManager consumerOnlineStateManager;

    @Mock
    private EnvProvider envProvider;

    @Mock
    private PullService pullService;

    @Mock
    private AckService ackService;

    @Mock
    private BrokerService brokerService;

    @Mock
    private MetaInfoService metaInfoService;

    @Mock
    private SendMessageBack sendMessageBack;

    @Mock
    private ExecutorService executorService;

    private PullEntryManager pullEntryManager;

    @Before
    public void before() throws Exception {
        mockStatic(ConsumeMessageExecutorFactory.class);
        this.pullEntryManager = new PullEntryManager(
                TEST_CLIENT_ID,
                consumerOnlineStateManager,
                envProvider,
                pullService,
                ackService,
                brokerService,
                metaInfoService,
                sendMessageBack,
                executorService
        );
    }

    @Test
    public void testUpdateClientWhenVersionIsTheSame() throws Exception {
        CompositePullClient pullClient = getCompositePullClient();
        List components = pullClient.getComponents();

        ConsumerMetaInfoResponse response = getConsumerMetaInfoResponse();

        Map<String, PullClient> clientMap = mock(Map.class);
        when(clientMap.get(anyString())).thenReturn(pullClient);
        Whitebox.setInternalState(pullEntryManager, "clientMap", clientMap);

        pullEntryManager.updateClient(response, null, false);
        assertEquals(components, pullClient.getComponents());
    }

    @Test
    public void testUpdateClientWhenThereIsNoOldClient() throws Exception {

        ConsumerMetaInfoResponse response = getConsumerMetaInfoResponse();
        String subject = response.getSubject();
        String consumerGroup = response.getConsumerGroup();

        pullEntryManager.updateClient(response, getRegistParam(), false);

        CompositePullClient compositePullClient = (CompositePullClient) pullEntryManager.getPullClient(subject, consumerGroup);
        List components = compositePullClient.getComponents();
        assertEquals(components.size(), 2);

        CompositePullClient normalPullClient = (CompositePullClient) components.get(0);
        CompositePullClient retryPullClient = (CompositePullClient) components.get(1);

        List<PullEntry> pullEntries = normalPullClient.getComponents();
        List<PullEntry> retryPullEntries = retryPullClient.getComponents();

        PullEntry pullEntry1 = pullEntries.get(0);
        assertEquals(pullEntry1.getSubject(), TEST_SUBJECT);
        assertEquals(pullEntry1.getPartitionName(), TEST_PARTITION_1);
        assertEquals(pullEntry1.getBrokerGroup(), TEST_BROKER_GROUP_1);

        PullEntry pullEntry2 = pullEntries.get(1);
        assertEquals(pullEntry2.getSubject(), TEST_SUBJECT);
        assertEquals(pullEntry2.getPartitionName(), TEST_PARTITION_2);
        assertEquals(pullEntry2.getBrokerGroup(), TEST_BROKER_GROUP_2);


        PullEntry retryPullEntry1 = retryPullEntries.get(0);
        assertEquals(retryPullEntry1.getSubject(), TEST_SUBJECT);
        assertEquals(retryPullEntry1.getPartitionName(), RetryPartitionUtils.buildRetryPartitionName(TEST_PARTITION_1, TEST_CONSUMER_GROUP));
        assertEquals(retryPullEntry1.getBrokerGroup(), TEST_BROKER_GROUP_1);

        PullEntry retryPullEntry2 = retryPullEntries.get(1);
        assertEquals(retryPullEntry2.getSubject(), TEST_SUBJECT);
        assertEquals(retryPullEntry2.getPartitionName(), RetryPartitionUtils.buildRetryPartitionName(TEST_PARTITION_2, TEST_CONSUMER_GROUP));
        assertEquals(retryPullEntry2.getBrokerGroup(), TEST_BROKER_GROUP_2);

    }
}
