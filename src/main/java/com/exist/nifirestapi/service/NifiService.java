package com.exist.nifirestapi.service;

import java.util.List;

import com.exist.nifirestapi.builder.ConnectionBuilder;
import com.exist.nifirestapi.client.NifiClient;

import org.apache.nifi.web.api.dto.ComponentDTO;
import org.apache.nifi.web.api.dto.RemoteProcessGroupPortDTO;
import org.apache.nifi.web.api.entity.ConnectionEntity;
import org.apache.nifi.web.api.entity.ControllerServiceEntity;
import org.apache.nifi.web.api.entity.FunnelEntity;
import org.apache.nifi.web.api.entity.Permissible;
import org.apache.nifi.web.api.entity.PortEntity;
import org.apache.nifi.web.api.entity.ProcessGroupEntity;
import org.apache.nifi.web.api.entity.ProcessorEntity;
import org.apache.nifi.web.api.entity.ProcessorsEntity;
import org.apache.nifi.web.api.entity.RemoteProcessGroupEntity;

public class NifiService {

    private NifiClient nifiClient;

    public NifiService(NifiClient nifiClient) {
        this.nifiClient = nifiClient;
    }

    public ProcessorEntity addProcessor(ProcessorEntity processor, String processGroupId) {
        return this.nifiClient.addProcessor(processor, processGroupId);
    }

    public <T extends Permissible<? extends ComponentDTO>> ConnectionEntity connectComponents(
        T source, 
        T destination, 
        List<String> connectionRelationships, 
        String processGroupId) {

        ConnectionEntity connection = new ConnectionBuilder()
            .source(source)
            .destination(destination)
            .connectionRelationships(connectionRelationships)
            .build();

    	return this.nifiClient.addConnection(connection, processGroupId);
    }

    public <T extends Permissible<? extends ComponentDTO>> ConnectionEntity connectToRemoteProcessGroup(
        T source, 
        RemoteProcessGroupEntity destination, 
        String portName,
        List<String> connectionRelationships, 
        String processGroupId) {

        RemoteProcessGroupPortDTO remoteInputPort = null;

        while (remoteInputPort == null) {
            remoteInputPort = nifiClient.getRemoteProcessGroup(destination.getId())
                                        .getComponent()
                                        .getContents()
                                        .getInputPorts()
                                        .stream()
                                        .filter(port -> port.getName().equals(portName))
                                        .findFirst()
                                        .orElse(null);
        }
            

        ConnectionEntity remoteConnection = new ConnectionBuilder()
            .source(source)
            .destinationId(remoteInputPort.getId())
            .destinationGroupId(remoteInputPort.getGroupId())
            .destinationType("REMOTE_INPUT_PORT")
            .connectionRelationships(connectionRelationships)
            .build();

        return this.nifiClient.addConnection(remoteConnection, processGroupId);
    }

    public ControllerServiceEntity addControllerService(ControllerServiceEntity controllerService, String processGroupId) {
        return this.nifiClient.addControllerService(controllerService, processGroupId);
    }

    public void enableControllerService(ControllerServiceEntity controllerService) {
        controllerService.getComponent().setState("ENABLED");
        controllerService.getComponent().setProperties(null);

        this.nifiClient.updateControllerService(controllerService);
    }

    public void updateControllerService(ControllerServiceEntity controllerService) {
        this.nifiClient.updateControllerService(controllerService);
    }

    public ProcessGroupEntity addProcessGroup(ProcessGroupEntity processGroup, String processGroupId) {
        return this.nifiClient.addProcessGroup(processGroup, processGroupId);
    }

    public RemoteProcessGroupEntity addRemoteProcessGroup(RemoteProcessGroupEntity remoteProcessGroup, String processGroupId) {
        return this.nifiClient.addRemoteProcessGroup(remoteProcessGroup, processGroupId);
    }

    public PortEntity addInputPort(PortEntity inputPort, String processGroupId) {
        return this.nifiClient.addInputPort(inputPort, processGroupId);
    }

    public PortEntity addOutputPort(PortEntity outputPort, String processGroupId) {
        return this.nifiClient.addOutputPort(outputPort, processGroupId);
    }

    public ProcessorsEntity getProcessors(String processGroupId) {
        return this.nifiClient.getProcessors(processGroupId);
    }

    public String construct(String apiPath) {
        return this.nifiClient.constructUrl(apiPath);
    }

    public RemoteProcessGroupEntity getRemoteProcessGroup(String remoteProcessGroupId) {
        return this.nifiClient.getRemoteProcessGroup(remoteProcessGroupId);
    }

    public FunnelEntity addFunnel(FunnelEntity funnel, String processGroupId) {
        return this.nifiClient.addFunnel(funnel, processGroupId);
    }

    public void enableRemoteProcessGroupTransmission(RemoteProcessGroupEntity remoteProcessGroup) {
        remoteProcessGroup.getComponent()
                          .setTransmitting(true);
                          
        this.nifiClient.updateRemoteProcessGroup(remoteProcessGroup);
    }
    
}
