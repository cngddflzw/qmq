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

package qunar.tc.qmq.meta.processor;

import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.base.ClientRequestType;
import qunar.tc.qmq.base.OnOfflineState;
import qunar.tc.qmq.codec.Serializer;
import qunar.tc.qmq.codec.Serializers;
import qunar.tc.qmq.common.ClientType;
import qunar.tc.qmq.concurrent.ActorSystem;
import qunar.tc.qmq.event.EventDispatcher;
import qunar.tc.qmq.meta.*;
import qunar.tc.qmq.meta.cache.CachedMetaInfoManager;
import qunar.tc.qmq.meta.cache.CachedOfflineStateManager;
import qunar.tc.qmq.meta.route.ReadonlyBrokerGroupManager;
import qunar.tc.qmq.meta.route.SubjectRouter;
import qunar.tc.qmq.meta.store.Store;
import qunar.tc.qmq.meta.utils.ClientLogUtils;
import qunar.tc.qmq.protocol.*;
import qunar.tc.qmq.protocol.consumer.ConsumerMetaInfoResponse;
import qunar.tc.qmq.protocol.consumer.MetaInfoRequest;
import qunar.tc.qmq.protocol.producer.ProducerMetaInfoResponse;
import qunar.tc.qmq.util.RemotingBuilder;
import qunar.tc.qmq.utils.PayloadHolderUtils;
import qunar.tc.qmq.utils.RetrySubjectUtils;
import qunar.tc.qmq.utils.SubjectUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * @author yunfeng.yang
 * @since 2017/9/1
 */
class ClientRegisterWorker implements ActorSystem.Processor<ClientRegisterProcessor.ClientRegisterMessage> {
    private static final Logger LOG = LoggerFactory.getLogger(ClientRegisterWorker.class);

    private final SubjectRouter subjectRouter;
    private final ActorSystem actorSystem;
    private final Store store;
    private final CachedOfflineStateManager offlineStateManager;
    private final ReadonlyBrokerGroupManager readonlyBrokerGroupManager;
    private final CachedMetaInfoManager cachedMetaInfoManager;

    ClientRegisterWorker(final SubjectRouter subjectRouter, final CachedOfflineStateManager offlineStateManager, final Store store, ReadonlyBrokerGroupManager readonlyBrokerGroupManager, CachedMetaInfoManager cachedMetaInfoManager) {
        this.subjectRouter = subjectRouter;
        this.readonlyBrokerGroupManager = readonlyBrokerGroupManager;
        this.cachedMetaInfoManager = cachedMetaInfoManager;
        this.actorSystem = new ActorSystem("qmq_meta");
        this.offlineStateManager = offlineStateManager;
        this.store = store;
    }

    void register(ClientRegisterProcessor.ClientRegisterMessage message) {
        actorSystem.dispatch("client_register_" + message.getMetaInfoRequest().getSubject(), message, this);
    }

    @Override
    public boolean process(ClientRegisterProcessor.ClientRegisterMessage message, ActorSystem.Actor<ClientRegisterProcessor.ClientRegisterMessage> self) {
        final MetaInfoRequest request = message.getMetaInfoRequest();

        final MetaInfoResponse response = handleClientRegister(request);
        writeResponse(message, response);
        return true;
    }

    private MetaInfoResponse handleClientRegister(final MetaInfoRequest request) {
        String realSubject = RetrySubjectUtils.getRealSubject(request.getSubject());
        int clientRequestType = request.getRequestType();
        OnOfflineState onOfflineState = request.getOnlineState();

        if (SubjectUtils.isInValid(realSubject)) {
            onOfflineState = OnOfflineState.OFFLINE;
            return buildResponse(request, -2, onOfflineState, new BrokerCluster(new ArrayList<>()));
        }

        try {
            if (ClientRequestType.ONLINE.getCode() == clientRequestType) {
                store.insertClientMetaInfo(request);
            }

            final List<BrokerGroup> brokerGroups = subjectRouter.route(realSubject, request);
            List<BrokerGroup> removedReadonlyGroups = readonlyBrokerGroupManager.disableReadonlyBrokerGroup(realSubject, request.getClientTypeCode(), brokerGroups);
            final List<BrokerGroup> filteredBrokerGroups = filterBrokerGroups(removedReadonlyGroups);
            final OnOfflineState clientState = offlineStateManager.queryClientState(request.getClientId(), request.getSubject(), request.getConsumerGroup());

            ClientLogUtils.log(realSubject,
                    "client register response, request:{}, realSubject:{}, brokerGroups:{}, clientState:{}",
                    request, realSubject, filteredBrokerGroups, clientState);

            return buildResponse(request, offlineStateManager.getLastUpdateTimestamp(), clientState, new BrokerCluster(filteredBrokerGroups));
        } catch (Exception e) {
            LOG.error("process exception. {}", request, e);
            onOfflineState = OnOfflineState.OFFLINE;
            return buildResponse(request, -2, onOfflineState, new BrokerCluster(new ArrayList<>()));
        } finally {
            if (ClientRequestType.HEARTBEAT.getCode() == clientRequestType || ClientRequestType.SWITCH_STATE.getCode() == clientRequestType) {
                // 上线的时候如果出现异常可能会将客户端上线状态改为下线
                request.setOnlineState(onOfflineState);
                EventDispatcher.dispatch(request);
            }
        }
    }

    private List<BrokerGroup> filterBrokerGroups(final List<BrokerGroup> brokerGroups) {
        return removeNrwBrokerGroup(brokerGroups);
    }

    private List<BrokerGroup> removeNrwBrokerGroup(final List<BrokerGroup> brokerGroups) {
        if (brokerGroups.isEmpty()) {
            return brokerGroups;
        }

        final List<BrokerGroup> result = new ArrayList<>();
        for (final BrokerGroup brokerGroup : brokerGroups) {
            if (brokerGroup.getBrokerState() != BrokerState.NRW) {
                result.add(brokerGroup);
            }
        }
        return result;
    }

    private MetaInfoResponse buildResponse(MetaInfoRequest clientRequest, long updateTime, OnOfflineState clientState, BrokerCluster brokerCluster) {
        ClientType clientType = ClientType.of(clientRequest.getRequestType());
        MetaInfoResponse response;
        String subject = clientRequest.getSubject();
        if (clientType.isProducer()) {
            response = new ProducerMetaInfoResponse();
            PartitionMapping partitionMapping = cachedMetaInfoManager.getPartitionMapping(subject);
            ((ProducerMetaInfoResponse) response).setPartitionMapping(partitionMapping);
        } else {
            PartitionAllocation partitionAllocation = cachedMetaInfoManager.getPartitionAllocation(subject, clientRequest.getConsumerGroup());
            response = new ConsumerMetaInfoResponse();
            ((ConsumerMetaInfoResponse) response).setPartitionAllocation(partitionAllocation);
        }
        response.setConsumerGroup(clientRequest.getConsumerGroup());
        response.setTimestamp(updateTime);
        response.setOnOfflineState(clientState);
        response.setSubject(clientRequest.getSubject());
        response.setClientTypeCode(clientRequest.getClientTypeCode());
        response.setBrokerCluster(brokerCluster);
        return response;
    }

    private void writeResponse(final ClientRegisterProcessor.ClientRegisterMessage message, final MetaInfoResponse response) {
        final RemotingHeader header = message.getHeader();
        final MetaInfoResponsePayloadHolder payloadHolder = createPayloadHolder(message, response);
        final Datagram datagram = RemotingBuilder.buildResponseDatagram(CommandCode.SUCCESS, header, payloadHolder);
        message.getCtx().writeAndFlush(datagram);
    }

    private MetaInfoResponsePayloadHolder createPayloadHolder(ClientRegisterProcessor.ClientRegisterMessage message, MetaInfoResponse response) {
        RemotingHeader header = message.getHeader();
        short version = header.getVersion();
        if (version >= RemotingHeader.VERSION_10) {
            if (response instanceof ProducerMetaInfoResponse) {
                return new ProducerMetaInfoResponsePayloadHolderV10((ProducerMetaInfoResponse) response);
            } else if (response instanceof ConsumerMetaInfoResponse) {
                return new ConsumerMetaInfoResponsePayloadHolderV10((ConsumerMetaInfoResponse) response);
            } else {
                throw new IllegalArgumentException(String.format("不支持的类型 %s", response.getClass()));
            }
        }
        return new MetaInfoResponsePayloadHolder(response);
    }

    private static class MetaInfoResponsePayloadHolder implements PayloadHolder {
        private final MetaInfoResponse response;

        MetaInfoResponsePayloadHolder(MetaInfoResponse response) {
            this.response = response;
        }

        @Override
        public void writeBody(ByteBuf out) {
            out.writeLong(response.getTimestamp());
            PayloadHolderUtils.writeString(response.getSubject(), out);
            PayloadHolderUtils.writeString(response.getConsumerGroup(), out);
            out.writeByte(response.getOnOfflineState().code());
            out.writeByte(response.getClientTypeCode());
            out.writeShort(response.getBrokerCluster().getBrokerGroups().size());
            writeBrokerCluster(out);
        }

        private void writeBrokerCluster(ByteBuf out) {
            for (BrokerGroup brokerGroup : response.getBrokerCluster().getBrokerGroups()) {
                PayloadHolderUtils.writeString(brokerGroup.getGroupName(), out);
                PayloadHolderUtils.writeString(brokerGroup.getMaster(), out);
                out.writeLong(brokerGroup.getUpdateTime());
                out.writeByte(brokerGroup.getBrokerState().getCode());
            }
        }
    }

    private static class ProducerMetaInfoResponsePayloadHolderV10 extends MetaInfoResponsePayloadHolder implements PayloadHolder {

        private final ProducerMetaInfoResponse response;

        ProducerMetaInfoResponsePayloadHolderV10(ProducerMetaInfoResponse response) {
            super(response);
            this.response = response;
        }

        @Override
        public void writeBody(ByteBuf out) {
            super.writeBody(out);
            PartitionMapping partitionMapping = response.getPartitionMapping();
            Serializer<PartitionMapping> serializer = Serializers.getSerializer(PartitionMapping.class);
            serializer.serialize(partitionMapping, out);
        }
    }

    private static class ConsumerMetaInfoResponsePayloadHolderV10 extends MetaInfoResponsePayloadHolder implements PayloadHolder {

        private final ConsumerMetaInfoResponse response;

        ConsumerMetaInfoResponsePayloadHolderV10(ConsumerMetaInfoResponse response) {
            super(response);
            this.response = response;
        }

        @Override
        public void writeBody(ByteBuf out) {
            super.writeBody(out);
            PartitionAllocation partitionAllocation = response.getPartitionAllocation();
            Serializer<PartitionAllocation> serializer = Serializers.getSerializer(PartitionAllocation.class);
            serializer.serialize(partitionAllocation, out);
        }
    }
}
