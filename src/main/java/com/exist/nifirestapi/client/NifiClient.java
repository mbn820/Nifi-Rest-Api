package com.exist.nifirestapi.client;

import org.apache.nifi.web.api.entity.ConnectionEntity;
import org.apache.nifi.web.api.entity.ControllerServiceEntity;
import org.apache.nifi.web.api.entity.PortEntity;
import org.apache.nifi.web.api.entity.ProcessGroupEntity;
import org.apache.nifi.web.api.entity.ProcessorEntity;
import org.apache.nifi.web.api.entity.ProcessorsEntity;
import org.springframework.web.client.RestTemplate;

public class NifiClient {

    private RestTemplate restTemplate;

    private String PROCESSOR_URL =
        "http://localhost:9090/nifi-api/process-groups/{process-group}/processors";

    private String CONNECTION_URL =
        "http://localhost:9090/nifi-api/process-groups/{process-group}/connections";

    private String CONTROLLER_SERVICE_URL =
        "http://localhost:9090/nifi-api/process-groups/{process-group}/controller-services";

    private String PROCESS_GROUP_URL = 
        "http://localhost:9090/nifi-api/process-groups/{process-group}/process-groups";

    private String INPUT_PORT_URL = 
        "http://localhost:9090/nifi-api/process-groups/{process-group}/input-ports";

    private String OUTPUT_PORT_URL = 
        "http://localhost:9090/nifi-api/process-groups/{process-group}/output-ports";

    private String UPDATE_CONTROLLER = 
        "http://localhost:9090/nifi-api/controller-services/{controller-id}";


    public NifiClient(RestTemplate restTemplate) {
        this.restTemplate = restTemplate;
    }

    public void setRestTemplate(RestTemplate restTemplate) {
        this.restTemplate = restTemplate;
    }

    public ProcessorEntity addProcessor(ProcessorEntity processor, String processGroupId) {
        return this.restTemplate.postForObject(PROCESSOR_URL, processor, ProcessorEntity.class, processGroupId);
    }

    public ConnectionEntity addConnection(ConnectionEntity connection, String processGroupId) {
        return this.restTemplate.postForObject(CONNECTION_URL, connection, ConnectionEntity.class, processGroupId);
    }

    public ControllerServiceEntity addControllerService(ControllerServiceEntity controllerService, String processGroupId) {
        return this.restTemplate.postForObject(CONTROLLER_SERVICE_URL, controllerService, ControllerServiceEntity.class, processGroupId);
    }

    public void updateControllerService(ControllerServiceEntity controllerService) {
        this.restTemplate.put(UPDATE_CONTROLLER, controllerService, controllerService.getId());
    }

    public ProcessGroupEntity addProcessGroup(ProcessGroupEntity processGroup, String processGroupId) {
        return this.restTemplate.postForObject(PROCESS_GROUP_URL, processGroup, ProcessGroupEntity.class, processGroupId);
    }

    public PortEntity addInputPort(PortEntity inputPort, String processGroupId) {
        return this.restTemplate.postForObject(INPUT_PORT_URL, inputPort, PortEntity.class, processGroupId);
    }

    public PortEntity addOutputPort(PortEntity outputPort, String processGroupId) {
        return this.restTemplate.postForObject(OUTPUT_PORT_URL, outputPort, PortEntity.class, processGroupId);
    }

    public ProcessorsEntity getProcessors(String processGroupId) {
        return this.restTemplate.getForObject(PROCESSOR_URL, ProcessorsEntity.class, processGroupId);
    }

}
