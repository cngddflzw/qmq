package qunar.tc.qmq.meta.web;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.CollectionUtils;
import qunar.tc.qmq.common.JsonUtils;
import qunar.tc.qmq.common.PartitionConstants;
import qunar.tc.qmq.jdbc.JdbcTemplateHolder;
import qunar.tc.qmq.meta.cache.CachedMetaInfoManager;
import qunar.tc.qmq.meta.cache.DefaultCachedMetaInfoManager;
import qunar.tc.qmq.meta.order.AveragePartitionAllocator;
import qunar.tc.qmq.meta.order.DefaultPartitionNameResolver;
import qunar.tc.qmq.meta.order.DefaultPartitionService;
import qunar.tc.qmq.meta.order.PartitionService;
import qunar.tc.qmq.meta.store.impl.*;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;

/**
 * Partition 扩容缩容接口
 *
 * @author zhenwei.liu
 * @since 2019-08-22
 */
public class PartitionManagementServlet extends HttpServlet {

    private static final Logger LOGGER = LoggerFactory.getLogger(PartitionManagementServlet.class);
    private static final ObjectMapper jsonMapper = JsonUtils.getMapper();
    private final PartitionService partitionService;
    private final CachedMetaInfoManager cachedMetaInfoManager;

    public PartitionManagementServlet() {
        DatabaseStore store = new DatabaseStore();
        this.partitionService = new DefaultPartitionService(
                new DefaultPartitionNameResolver(),
                store,
                new ClientMetaInfoStoreImpl(),
                new PartitionStoreImpl(),
                new PartitionSetStoreImpl(),
                new PartitionAllocationStoreImpl(),
                new AveragePartitionAllocator(),
                JdbcTemplateHolder.getTransactionTemplate()
        );
        this.cachedMetaInfoManager = new DefaultCachedMetaInfoManager(store, new ReadonlyBrokerGroupSettingStoreImpl(JdbcTemplateHolder.getOrCreate()), partitionService);
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        String subject = req.getParameter("subject");
        String physicalPartitionNumStr = req.getParameter("physicalPartitionNum");
        int physicalPartitionNum = physicalPartitionNumStr == null ?
                PartitionConstants.DEFAULT_PHYSICAL_PARTITION_NUM : Integer.valueOf(physicalPartitionNumStr);

        resp.setStatus(HttpServletResponse.SC_OK);
        resp.setHeader("Content-Type", "application/json");
        PrintWriter writer = resp.getWriter();
        try {
            List<String> brokerGroups = cachedMetaInfoManager.getBrokerGroups(subject);
            if (CollectionUtils.isEmpty(brokerGroups)) {
                throw new IllegalArgumentException("subject broker group 未初始化, 请先发送一条消息");
            }
            partitionService.updatePartitions(subject, physicalPartitionNum, brokerGroups);
            writer.println(jsonMapper.writeValueAsString(new JsonResult<>(ResultStatus.OK, "成功", null)));
        } catch (Throwable t) {
            LOGGER.error("顺序消息分配失败 {}", subject, t);
            writer.println(jsonMapper.writeValueAsString(new JsonResult<>(ResultStatus.SYSTEM_ERROR, String.format("失败 %s", t.getMessage()), null)));
        }

    }
}
