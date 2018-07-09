package com.exist.nifirestapi.client;

import org.apache.nifi.web.api.entity.ConnectionEntity;
import org.apache.nifi.web.api.entity.ControllerServiceEntity;
import org.apache.nifi.web.api.entity.ProcessGroupEntity;
import org.apache.nifi.web.api.entity.ProcessorEntity;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

@Component
public class NifiClient {

    @Autowired
    private RestTemplate restTemplate;

    private static final String CREATE_PROCESSOR_URL =
        "http://localhost:9090/nifi-api/process-groups/{process-group}/processors";

    private static final String CREATE_CONNECTION_URL =
        "http://localhost:9090/nifi-api/process-groups/{process-group}/connections";

    private static final String CREATE_CONTROLLER_SERVICE_URL =
        "http://localhost:9090/nifi-api/process-groups/{process-group}/controller-services";

    private static final String CREATE_PROCESS_GROUP_URL = 
        "http://localhost:9090/nifi-api/process-groups/{process-group}/process-groups";

    private static final String TEST_URL =
        "http://localhost:7777/contentListener";


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

}
