package com.exist.nifirestapi;

import com.alibaba.fastjson.JSONObject;
import com.exist.nifirestapi.client.NifiClient;
import com.exist.nifirestapi.service.NifiService;
import com.exist.nifirestapi.util.Comm;
import org.apache.nifi.web.api.dto.ControllerServiceDTO;
import org.apache.nifi.web.api.dto.RevisionDTO;
import org.apache.nifi.web.api.entity.ComponentEntity;
import org.apache.nifi.web.api.entity.ControllerServiceEntity;
import org.apache.nifi.web.api.entity.ProcessorEntity;
import org.apache.nifi.web.api.entity.UpdateControllerServiceReferenceRequestEntity;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.nifi.web.api.entity.ScheduleComponentsEntity.STATE_DISABLED;
import static org.apache.nifi.web.api.entity.ScheduleComponentsEntity.STATE_ENABLED;
import static org.apache.nifi.web.api.entity.ScheduleComponentsEntity.STATE_STOPPED;

@RunWith(SpringRunner.class)
@SpringBootTest
public class NifiRestApiApplicationTests {

    @Autowired
    private NifiService nifiService;

    @Autowired
    private NifiClient nifiClient;

    @Autowired
    private Comm comm;

    @Test
    public void contextLoads() {
	    /*
	    * 5633a3c7-0165-1000-c6f2-e59694541768
5633a3df-0165-1000-967f-e11e96278589
	    * */


        List<ControllerServiceEntity> controllerServiceEntities = new ArrayList<>();
        controllerServiceEntities.add(nifiService.getControllerServiceInfo("566cfc3f-0165-1000-ed0a-75ae6b50348f"));
        for (ControllerServiceEntity controllerServiceEntity : controllerServiceEntities) {
            ControllerServiceDTO component = controllerServiceEntity.getComponent();
            component.setState("ENABLED"); // // ENABLED DISABLED
            controllerServiceEntity.setComponent(component);
            System.out.println(controllerServiceEntity.getRevision().getClientId());
            controllerServiceEntity.getRevision().setClientId("13");
            System.out.println(controllerServiceEntity.getRevision().getClientId());
            if (controllerServiceEntity.getRevision().getVersion() == null) {
                controllerServiceEntity.getRevision().setVersion((long) 0);
            }
            System.out.println(controllerServiceEntity.getRevision().getVersion());
            controllerServiceEntity.setDisconnectedNodeAcknowledged(false);


        }

    }

    @Test
    public void getversion() {
        System.out.println(nifiService.getControllerServiceInfo("566cfc3f-0165-1000-ed0a-75ae6b50348f").getRevision().getVersion());

    }

    @Test
    public void json() {
        //ControllerServiceEntity controllerServiceEntity = nifiService.getControllerServiceInfo("5b515071-0165-1000-dd90-a2920d8ab972");
        //controllerServiceEntity.getComponent().getProperties().put("Database Connection URL","jdbc:mysql://192.168.1.7:3306/test?useUnicode=true&characterEncoding=utf-8");
        RevisionDTO revision = nifiService.getControllerServiceInfo("5b515071-0165-1000-dd90-a2920d8ab972").getRevision();
        Map<String, Object> revisionVar = new HashMap<>();
        revisionVar.put("clientId", revision.getClientId() == null ? "0-0" : revision.getClientId());
        revisionVar.put("version", revision.getVersion());

        Map<String, Object> componentVar = new HashMap<>();
        componentVar.put("id", "5b515071-0165-1000-dd90-a2920d8ab972");
        componentVar.put("name", "MYSQL_125_DBCP_TEST");
        Map<String, Object> propertiesVar = new HashMap<>();
        propertiesVar.put("Database Connection URL", "jdbc:mysql://192.168.1.17:3306/test?useUnicode=true&characterEncoding=utf-8");
        componentVar.put("properties", propertiesVar);


        Map<String, Object> urlVar = new HashMap<>();
        urlVar.put("revision", revisionVar);
        urlVar.put("component", componentVar);
        urlVar.put("disconnectedNodeAcknowledged", false);

        JSONObject json = new JSONObject(urlVar);
        System.out.println(json.toJSONString());

        String url = "http://192.168.1.7:8080/nifi-api/controller-services/5b515071-0165-1000-dd90-a2920d8ab972";
        comm.httpUrlConnectionPut(url, json.toJSONString());
    }

    @Test
    public void startProcess() {

        // http://192.168.1.7:8080/nifi-api/processors/
        //  allowableValues = "RUNNING, STOPPED, DISABLED"
        RevisionDTO revision = nifiService.getProcessorInfo("5b65ef80-0165-1000-9f45-1f4c68f75bcc").getRevision();
        Map<String, Object> revisionVar = new HashMap<>();
        revisionVar.put("clientId", revision.getClientId() == null ? "0-0" : revision.getClientId());
        revisionVar.put("version", revision.getVersion());

        Map<String, Object> componentVar = new HashMap<>();
        componentVar.put("id", "5b65ef80-0165-1000-9f45-1f4c68f75bcc");
        componentVar.put("state", "RUNNING");

        Map<String, Object> urlVar = new HashMap<>();

        urlVar.put("revision", revisionVar);
        urlVar.put("component", componentVar);
        urlVar.put("disconnectedNodeAcknowledged", false);

        JSONObject json = new JSONObject(urlVar);
        System.out.println(json.toJSONString());
        String url = "http://192.168.1.7:8080/nifi-api/processors/" + "5b65ef80-0165-1000-9f45-1f4c68f75bcc";
        comm.httpUrlConnectionPut(url, json.toJSONString());


    }

    @Test
    public void configProcess() {

        ProcessorEntity processorEntity = nifiService.getProcessorInfo("5b65ef6f-0165-1000-7b17-2e971c5a538a");

        Map<String, Object> configVar = new HashMap<>();
        System.out.println(processorEntity.getComponent().getConfig().getSchedulingStrategy());
        configVar.put("schedulingStrategy",processorEntity.getComponent().getConfig().getSchedulingStrategy());
        configVar.put("autoTerminatedRelationships", processorEntity.getComponent().getConfig().getAutoTerminatedRelationships());
        configVar.put("bulletinLevel",processorEntity.getComponent().getConfig().getBulletinLevel());
        configVar.put("comments",processorEntity.getComponent().getConfig().getComments());
        configVar.put("concurrentlySchedulableTaskCount",processorEntity.getComponent().getConfig().getConcurrentlySchedulableTaskCount());
        configVar.put("executionNode",processorEntity.getComponent().getConfig().getExecutionNode());
        configVar.put("penaltyDuration",processorEntity.getComponent().getConfig().getPenaltyDuration());
        configVar.put("runDurationMillis",processorEntity.getComponent().getConfig().getRunDurationMillis());
        configVar.put("schedulingPeriod",processorEntity.getComponent().getConfig().getSchedulingPeriod());
        configVar.put("yieldDuration",processorEntity.getComponent().getConfig().getYieldDuration());

        Map<String, Object> propertiesVar = new HashMap<>();
        propertiesVar.put("Table Name","name1111");
        configVar.put("properties",propertiesVar);

        Map<String, Object> componentMap = new HashMap<>();
        componentMap.put("config",configVar);
        componentMap.put("id",processorEntity.getId());
        componentMap.put("name",processorEntity.getComponent().getName());
        componentMap.put("state",processorEntity.getComponent().getState());


        Map<String, Object> revisionVar = new HashMap<>();
        revisionVar.put("clientId", processorEntity.getRevision().getClientId() == null ? "0-0" : processorEntity.getRevision().getClientId());
        revisionVar.put("version", processorEntity.getRevision().getVersion());

        Map<String, Object> urlVar = new HashMap<>();
        urlVar.put("component",componentMap);
        urlVar.put("disconnectedNodeAcknowledged",false);
        urlVar.put("revision",revisionVar);

        JSONObject json = new JSONObject(urlVar);
        System.out.println(json.toJSONString());
        String url = "http://192.168.1.7:8080/nifi-api/processors/" + "5b65ef6f-0165-1000-7b17-2e971c5a538a";
        System.out.println(comm.httpUrlConnectionPut(url, json.toJSONString()));
    }

}
