package com.exist.nifirestapi.service;

import com.alibaba.fastjson.JSONObject;
import com.exist.nifirestapi.builder.ConnectionBuilder;
import com.exist.nifirestapi.client.NifiClient;

import com.exist.nifirestapi.util.Comm;
import org.apache.nifi.web.api.dto.RevisionDTO;
import org.apache.nifi.web.api.entity.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

import static org.apache.nifi.web.api.entity.ScheduleComponentsEntity.STATE_ENABLED;

@Component
public class NifiService {

    @Autowired
    private NifiClient nifiClient;
    @Autowired
    private Comm comm;


    public ProcessorEntity addProcessor(ProcessorEntity processor) {
        return this.nifiClient.addProcessor(processor);
    }

    public ConnectionEntity connectProcessors(
            ProcessorEntity source, ProcessorEntity destination, String... connectionRelationships) {

        ConnectionBuilder connectionBuilder = new ConnectionBuilder()
                .source(source)
                .destination(destination);

        for (String connectionRelationship : connectionRelationships) {
            connectionBuilder.addConnectionRelationship(connectionRelationship);
        }

        ConnectionEntity connection = connectionBuilder.build();

        return this.nifiClient.addConnection(connection);
    }

    public ControllerServiceEntity addControllerService(ControllerServiceEntity controllerService) {
        return this.nifiClient.addControllerService(controllerService);
    }

    public ProcessGroupEntity addProcessGroup(ProcessGroupEntity processGroup) {
        return this.nifiClient.addProcessGroup(processGroup);
    }

    public ProcessorEntity getProcessorInfo(String id) {
        return this.nifiClient.getProcessorInfo(id);
    }

    public ControllerServiceEntity getControllerServiceInfo(String id) {
        return this.nifiClient.getControllerServiceInfo(id);
    }


    // 停止或者启动 controller
    public String stopOrStartControllerService(String state, ControllerServiceEntity controllerServiceEntity) {
        RevisionDTO revision = controllerServiceEntity.getRevision();
        Map<String, Object> revisionVar = new HashMap<>();
        revisionVar.put("clientId", revision.getClientId() == null ? "0-0" : revision.getClientId());
        revisionVar.put("version", revision.getVersion());

        Map<String, Object> componentVar = new HashMap<>();
        componentVar.put("id", controllerServiceEntity.getId());
        componentVar.put("state", state);

        Map<String, Object> urlVar = new HashMap<>();

        urlVar.put("revision", revisionVar);
        urlVar.put("component", componentVar);
        urlVar.put("disconnectedNodeAcknowledged", false);
        JSONObject json = new JSONObject(urlVar);

        return this.comm.httpUrlConnectionPut(this.nifiClient.getControllerServerUrl() + controllerServiceEntity.getId(), json.toJSONString());
    }

    // 修改controller的配置文件
    public void updateControlServiceConfig(ControllerServiceEntity controllerServiceEntity, Map<String, Object> propertiesVar) {
        RevisionDTO revision = controllerServiceEntity.getRevision();
        Map<String, Object> revisionVar = new HashMap<>();
        revisionVar.put("clientId", revision.getClientId() == null ? "0-0" : revision.getClientId());
        revisionVar.put("version", revision.getVersion());

        Map<String, Object> componentVar = new HashMap<>();
        componentVar.put("id", controllerServiceEntity.getId());
        componentVar.put("name", controllerServiceEntity.getComponent().getName());
        componentVar.put("properties", propertiesVar);

        Map<String, Object> urlVar = new HashMap<>();

        urlVar.put("revision", revisionVar);
        urlVar.put("component", componentVar);
        urlVar.put("disconnectedNodeAcknowledged", false);
        JSONObject json = new JSONObject(urlVar);

        this.comm.httpUrlConnectionPut(this.nifiClient.getControllerServerUrl() + controllerServiceEntity.getId(), json.toJSONString());
    }

    // 停止或者启动 processor
    public String stopOrStartProcessor(String state, ProcessorEntity processorEntity) {
        // http://192.168.1.7:8080/nifi-api/processors/
        //  allowableValues = "RUNNING, STOPPED, DISABLED"
        RevisionDTO revision = processorEntity.getRevision();
        Map<String, Object> revisionVar = new HashMap<>();
        revisionVar.put("clientId", revision.getClientId() == null ? "0-0" : revision.getClientId());
        revisionVar.put("version", revision.getVersion());

        Map<String, Object> componentVar = new HashMap<>();
        componentVar.put("id", processorEntity.getId());
        componentVar.put("state", state);

        Map<String, Object> urlVar = new HashMap<>();

        urlVar.put("revision", revisionVar);
        urlVar.put("component", componentVar);
        urlVar.put("disconnectedNodeAcknowledged", false);
        JSONObject json = new JSONObject(urlVar);

        return comm.httpUrlConnectionPut(this.nifiClient.getProcessorUrl() + processorEntity.getId(), json.toJSONString());
    }

    // 修改processor配置文件
    public String updateProcessorsCofing(ProcessorEntity processorEntity, Map<String, Object> propertiesVar) {

        Map<String, Object> configVar = new HashMap<>();
        System.out.println(processorEntity.getComponent().getConfig().getSchedulingStrategy());
        configVar.put("schedulingStrategy", processorEntity.getComponent().getConfig().getSchedulingStrategy());
        configVar.put("autoTerminatedRelationships", processorEntity.getComponent().getConfig().getAutoTerminatedRelationships());
        configVar.put("bulletinLevel", processorEntity.getComponent().getConfig().getBulletinLevel());
        configVar.put("comments", processorEntity.getComponent().getConfig().getComments());
        configVar.put("concurrentlySchedulableTaskCount", processorEntity.getComponent().getConfig().getConcurrentlySchedulableTaskCount());
        configVar.put("executionNode", processorEntity.getComponent().getConfig().getExecutionNode());
        configVar.put("penaltyDuration", processorEntity.getComponent().getConfig().getPenaltyDuration());
        configVar.put("runDurationMillis", processorEntity.getComponent().getConfig().getRunDurationMillis());
        configVar.put("schedulingPeriod", processorEntity.getComponent().getConfig().getSchedulingPeriod());
        configVar.put("yieldDuration", processorEntity.getComponent().getConfig().getYieldDuration());

        configVar.put("properties", propertiesVar);

        Map<String, Object> componentMap = new HashMap<>();
        componentMap.put("config", configVar);
        componentMap.put("id", processorEntity.getId());
        componentMap.put("name", processorEntity.getComponent().getName());
        componentMap.put("state", processorEntity.getComponent().getState());


        Map<String, Object> revisionVar = new HashMap<>();
        revisionVar.put("clientId", processorEntity.getRevision().getClientId() == null ? "0-0" : processorEntity.getRevision().getClientId());
        revisionVar.put("version", processorEntity.getRevision().getVersion());

        Map<String, Object> urlVar = new HashMap<>();
        urlVar.put("component", componentMap);
        urlVar.put("disconnectedNodeAcknowledged", false);
        urlVar.put("revision", revisionVar);

        JSONObject json = new JSONObject(urlVar);
        return comm.httpUrlConnectionPut(this.nifiClient.getProcessorUrl() + processorEntity.getId(), json.toJSONString());
    }

}
