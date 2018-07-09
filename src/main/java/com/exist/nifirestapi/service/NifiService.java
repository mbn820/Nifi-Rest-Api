package com.exist.nifirestapi.service;

import com.exist.nifirestapi.builder.ConnectionBuilder;
import com.exist.nifirestapi.client.NifiClient;

import org.apache.nifi.web.api.entity.ConnectionEntity;
import org.apache.nifi.web.api.entity.ControllerServiceEntity;
import org.apache.nifi.web.api.entity.ProcessGroupEntity;
import org.apache.nifi.web.api.entity.ProcessorEntity;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class NifiService {

    @Autowired
    private NifiClient nifiClient;

    public ProcessorEntity addProcessor(ProcessorEntity processor) {
        return this.nifiClient.addProcessor(processor);
    }

    public ConnectionEntity connectProcessors(
        ProcessorEntity source, ProcessorEntity destination, String... connectionRelationships) {

        ConnectionBuilder connectionBuilder = new ConnectionBuilder()
            .source(source)
            .destination(destination);

        for(String connectionRelationship : connectionRelationships) {
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
}
