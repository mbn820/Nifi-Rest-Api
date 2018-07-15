package com.exist.nifirestapi.client;

import org.apache.nifi.web.api.entity.ConnectionEntity;
import org.apache.nifi.web.api.entity.ControllerServiceEntity;
import org.apache.nifi.web.api.entity.PortEntity;
import org.apache.nifi.web.api.entity.ProcessGroupEntity;
import org.apache.nifi.web.api.entity.ProcessorEntity;
import org.apache.nifi.web.api.entity.ProcessorsEntity;
import org.apache.nifi.web.api.entity.RemoteProcessGroupEntity;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;

public class NifiClient {

    private RestTemplate restTemplate;

    private String host;
    private int port;

    private String PROCESSOR_URL;
    private String CONNECTION_URL;
    private String CONTROLLER_SERVICE_URL;
    private String PROCESS_GROUP_URL;
    private String REMOTE_PROCESS_GROUP_URL;
    private String INPUT_PORT_URL;
    private String OUTPUT_PORT_URL;
    private String UPDATE_CONTROLLER;
    private String GET_REMOTE;

    private String TEST_URL = "http://localhost:7777/contentListener";


    public NifiClient(String host, int port, RestTemplate restTemplate) {
        this.host = host;
        this.port = port;
        this.restTemplate = restTemplate;

        init();
    }

    public void init() {
        PROCESSOR_URL            = constructUrl("/process-groups/{process-group}/processors");
        CONNECTION_URL           = constructUrl("/process-groups/{process-group}/connections");
        CONTROLLER_SERVICE_URL   = constructUrl("/process-groups/{process-group}/controller-services");
        PROCESS_GROUP_URL        = constructUrl("/process-groups/{process-group}/process-groups");
        REMOTE_PROCESS_GROUP_URL = constructUrl("/process-groups/{process-group}/remote-process-groups");
        INPUT_PORT_URL           = constructUrl("/process-groups/{process-group}/input-ports");
        OUTPUT_PORT_URL          = constructUrl("/process-groups/{process-group}/output-ports");
        UPDATE_CONTROLLER        = constructUrl("/controller-services/{controller-id}");
        GET_REMOTE               = constructUrl("/remote-process-groups/{id}");
    }

    public String constructUrl(String apiPath) {
        return UriComponentsBuilder.newInstance()
            .scheme("http")
            .host(this.host)
            .port(this.port)
            .path("/nifi-api")
            .path(apiPath)
            .build()
            .toString();
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

    public RemoteProcessGroupEntity addRemoteProcessGroup(RemoteProcessGroupEntity remoteProcessGroup, String processGroupId) {
        return this.restTemplate.postForObject(REMOTE_PROCESS_GROUP_URL, remoteProcessGroup, RemoteProcessGroupEntity.class, processGroupId);
    }

    public void updateRemoteProcessGroup(RemoteProcessGroupEntity remoteProcessGroup) {
        this.restTemplate.put(GET_REMOTE, remoteProcessGroup, remoteProcessGroup.getId());
    }

    public RemoteProcessGroupEntity getRemoteProcessGroup(String remoteProcessGroupId) {
        return this.restTemplate.getForObject(GET_REMOTE, RemoteProcessGroupEntity.class, remoteProcessGroupId);
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
