package com.exist.nifirestapi.service;

import java.util.List;

import com.exist.nifirestapi.builder.ConnectionBuilder;
import com.exist.nifirestapi.client.NifiClient;

import org.apache.nifi.web.api.entity.ConnectionEntity;
import org.apache.nifi.web.api.entity.ControllerServiceEntity;
import org.apache.nifi.web.api.entity.PortEntity;
import org.apache.nifi.web.api.entity.ProcessGroupEntity;
import org.apache.nifi.web.api.entity.ProcessorEntity;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class NifiService {

    @Autowired
    private NifiClient nifiClient;

    public ProcessorEntity addProcessor(ProcessorEntity processor, String processGroupId) {
        return this.nifiClient.addProcessor(processor, processGroupId);
    }

    public ConnectionEntity connectProcessors(
        ProcessorEntity source, ProcessorEntity destination, List<String> connectionRelationships, String processGroupId) {

        ConnectionBuilder connectionBuilder = new ConnectionBuilder()
            .source(source)
            .destination(destination);

        for(String connectionRelationship : connectionRelationships) {
            connectionBuilder.addConnectionRelationship(connectionRelationship);
        }

        ConnectionEntity connection = connectionBuilder.build();

    	return this.nifiClient.addConnection(connection, processGroupId);
    }

    public ConnectionEntity connectProcessorToPort(
        ProcessorEntity source, PortEntity destination, List<String> connectionRelationships, String processGroupId) {

        ConnectionBuilder connectionBuilder = new ConnectionBuilder()
            .source(source)
            .destination(destination);
            
        for(String connectionRelationship : connectionRelationships) {
            connectionBuilder.addConnectionRelationship(connectionRelationship);
        }

        ConnectionEntity connection = connectionBuilder.build();

    	return this.nifiClient.addConnection(connection, processGroupId);
    }

    public ConnectionEntity connectPortToProcessor(
        PortEntity source, ProcessorEntity destination, List<String> connectionRelationships, String processGroupId) {

        ConnectionBuilder connectionBuilder = new ConnectionBuilder()
            .source(source)
            .destination(destination);
            
        for(String connectionRelationship : connectionRelationships) {
            connectionBuilder.addConnectionRelationship(connectionRelationship);
        }

        ConnectionEntity connection = connectionBuilder.build();

    	return this.nifiClient.addConnection(connection, processGroupId);
    }

    public ConnectionEntity connectPorts(
        PortEntity source, PortEntity destination, List<String> connectionRelationships, String processGroupId) {

        ConnectionBuilder connectionBuilder = new ConnectionBuilder()
            .source(source)
            .destination(destination);
            
        for(String connectionRelationship : connectionRelationships) {
            connectionBuilder.addConnectionRelationship(connectionRelationship);
        }

        ConnectionEntity connection = connectionBuilder.build();

    	return this.nifiClient.addConnection(connection, processGroupId);
    }

    public ControllerServiceEntity addControllerService(ControllerServiceEntity controllerService, String processGroupId) {
        return this.nifiClient.addControllerService(controllerService, processGroupId);
    }

    public ProcessGroupEntity addProcessGroup(ProcessGroupEntity processGroup, String processGroupId) {
        return this.nifiClient.addProcessGroup(processGroup, processGroupId);
    }

    public PortEntity addInputPort(PortEntity inputPort, String processGroupId) {
        return this.nifiClient.addInputPort(inputPort, processGroupId);
    }

    public PortEntity addOutputPort(PortEntity outputPort, String processGroupId) {
        return this.nifiClient.addOutputPort(outputPort, processGroupId);
    }
}
