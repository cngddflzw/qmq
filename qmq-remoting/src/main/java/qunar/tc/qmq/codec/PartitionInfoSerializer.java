package qunar.tc.qmq.codec;

import com.google.common.collect.RangeMap;
import io.netty.buffer.ByteBuf;
import qunar.tc.qmq.meta.PartitionInfo;
import qunar.tc.qmq.utils.PayloadHolderUtils;

/**
 * @author zhenwei.liu
 * @since 2019-08-23
 */
public class PartitionInfoSerializer implements Serializer<PartitionInfo> {

    private static final byte NULL = 0;
    private static final byte EXISTS = 1;

    @Override
    public void serialize(PartitionInfo partitionInfo, ByteBuf buf) {

        if (partitionInfo == null) {
            buf.writeByte(NULL);
            return;
        } else {
            buf.writeByte(EXISTS);
        }

        PayloadHolderUtils.writeString(partitionInfo.getSubject(), buf);

        buf.writeInt(partitionInfo.getVersion());
        buf.writeInt(partitionInfo.getLogicalPartitionNum());

        Serializer<RangeMap> rangeMapSerializer = Serializers.getSerializer(RangeMap.class);
        rangeMapSerializer.serialize(partitionInfo.getLogical2PhysicalPartition(), buf);
        rangeMapSerializer.serialize(partitionInfo.getPhysicalPartition2Broker(), buf);
        rangeMapSerializer.serialize(partitionInfo.getPhysicalPartition2DelayBroker(), buf);
    }

    @Override
    public PartitionInfo deserialize(ByteBuf buf, Class... classes) {
        byte exists = buf.readByte();
        if (exists == NULL) {
            return null;
        }

        PartitionInfo partitionInfo = new PartitionInfo();
        partitionInfo.setSubject(PayloadHolderUtils.readString(buf));
        partitionInfo.setVersion(buf.readInt());
        partitionInfo.setLogicalPartitionNum(buf.readInt());

        Serializer<RangeMap> rangeMapSerializer = Serializers.getSerializer(RangeMap.class);
        partitionInfo.setLogical2PhysicalPartition(rangeMapSerializer.deserialize(buf, Integer.class, Integer.class));
        partitionInfo.setPhysicalPartition2Broker(rangeMapSerializer.deserialize(buf, Integer.class, String.class));
        partitionInfo.setPhysicalPartition2DelayBroker(rangeMapSerializer.deserialize(buf, Integer.class, String.class));

        return partitionInfo;
    }
}
