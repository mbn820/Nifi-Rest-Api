package com.exist.nifirestapi.client;

import com.alibaba.fastjson.JSONObject;
import org.apache.nifi.web.api.dto.RevisionDTO;
import org.apache.nifi.web.api.entity.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.RestTemplate;

import java.util.HashMap;
import java.util.Map;

import static org.apache.nifi.web.api.entity.ActivateControllerServicesEntity.STATE_DISABLED;
import static org.apache.nifi.web.api.entity.ScheduleComponentsEntity.STATE_STOPPED;

@Component
public class NifiClient {

    @Autowired
    private RestTemplate restTemplate;

    private static final String CREATE_PROCESSOR_URL =
            "http://192.168.1.7:8080/nifi-api/process-groups/{process-group}/processors";

    private static final String CREATE_CONNECTION_URL =
            "http://192.168.1.7:8080/nifi-api/process-groups/{process-group}/connections";

    private static final String CREATE_CONTROLLER_SERVICE_URL =
            "http://192.168.1.7:8080/nifi-api/process-groups/{process-group}/controller-services";

    private static final String CREATE_PROCESS_GROUP_URL =
            "http://192.168.1.7:8080/nifi-api/process-groups/{process-group}/process-groups";

    private static final String CONTORLLER_PROCESSORS_URL =
            "http://192.168.1.7:8080/nifi-api/processors/{id}";

    private static final String CONTROLLER_SERVICES_URL =
            "http://192.168.1.7:8080/nifi-api/controller-services/{id}";

    private static final String CONTROLLER_SERVICES_REFERENCES_URL =
            "http://192.168.1.7:8080/nifi-api/controller-services/{id}/references";


    private static final String CONTROLLER_SERVER_URL =
            "http://192.168.1.7:8080/nifi-api/controller-services/";

    private static final String PROCESSOR_URL =
            "http://192.168.1.7:8080/nifi-api/processors/";

    public ProcessorEntity addProcessor(ProcessorEntity processor) {
        return this.restTemplate.postForObject(CREATE_PROCESSOR_URL, processor, ProcessorEntity.class, "root");
    }

    public ConnectionEntity addConnection(ConnectionEntity connection) {
        return this.restTemplate.postForObject(CREATE_CONNECTION_URL, connection, ConnectionEntity.class, "root");
    }

    public ControllerServiceEntity addControllerService(ControllerServiceEntity controllerService) {
        return this.restTemplate.postForObject(CREATE_CONTROLLER_SERVICE_URL, controllerService, ControllerServiceEntity.class, "root");
    }

    public ProcessGroupEntity addProcessGroup(ProcessGroupEntity processGroup) {
        return this.restTemplate.postForObject(CREATE_PROCESS_GROUP_URL, processGroup, ProcessGroupEntity.class, "root");
    }

    public ProcessorEntity getProcessorInfo(String id){
            return this.restTemplate.getForObject(CONTORLLER_PROCESSORS_URL,ProcessorEntity.class,id);
    }

    public void updateProcessor(String id,ProcessorEntity processorEntity){
        this.restTemplate.put(CONTORLLER_PROCESSORS_URL,processorEntity,id);
    }

    public ControllerServiceEntity getControllerServiceInfo(String id){
        return this.restTemplate.getForObject(CONTROLLER_SERVICES_URL,ControllerServiceEntity.class,id);
    }

    public void updateControllerService(String id,ControllerServiceEntity controllerServiceEntity){
         this.restTemplate.put(CONTROLLER_SERVICES_URL,controllerServiceEntity,id);
    }

/*    public ControllerServiceReferencingComponentsEntity getControllerService(String id){
        return this.restTemplate.getForObject(CONTROLLER_SERVICES_REFERENCES_URL,ControllerServiceReferencingComponentsEntity.class,id);
    }
*/
    public void updateControllerService(String id,UpdateControllerServiceReferenceRequestEntity updateControllerServiceReferenceRequestEntity){
         this.restTemplate.put(CONTROLLER_SERVICES_REFERENCES_URL,updateControllerServiceReferenceRequestEntity,id);
    }

    public static String getControllerServerUrl() {
        return CONTROLLER_SERVER_URL;
    }

    public static String getProcessorUrl() {
        return PROCESSOR_URL;
    }
}
