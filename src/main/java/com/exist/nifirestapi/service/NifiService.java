package com.exist.nifirestapi.service;

import java.util.List;

import com.exist.nifirestapi.builder.ConnectionBuilder;
import com.exist.nifirestapi.client.NifiClient;

import org.apache.nifi.web.api.dto.ComponentDTO;
import org.apache.nifi.web.api.entity.ComponentEntity;
import org.apache.nifi.web.api.entity.ConnectionEntity;
import org.apache.nifi.web.api.entity.ControllerServiceEntity;
import org.apache.nifi.web.api.entity.Permissible;
import org.apache.nifi.web.api.entity.PortEntity;
import org.apache.nifi.web.api.entity.ProcessGroupEntity;
import org.apache.nifi.web.api.entity.ProcessorEntity;
import org.apache.nifi.web.api.entity.ProcessorsEntity;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class NifiService {

    @Autowired
    private NifiClient nifiClient;

    public ProcessorEntity addProcessor(ProcessorEntity processor, String processGroupId) {
        return this.nifiClient.addProcessor(processor, processGroupId);
    }

    public <T extends ComponentEntity & Permissible<? extends ComponentDTO>> ConnectionEntity connectComponents(
        T source, T destination, List<String> connectionRelationships, String processGroupId) {

        ConnectionBuilder connectionBuilder = new ConnectionBuilder()
            .source(source)
            .destination(destination);

        connectionRelationships.forEach(rel -> {
            connectionBuilder.addConnectionRelationship(rel);
        });

        ConnectionEntity connection = connectionBuilder.build();

    	return this.nifiClient.addConnection(connection, processGroupId);
    }

    public ControllerServiceEntity addControllerService(ControllerServiceEntity controllerService, String processGroupId) {
        return this.nifiClient.addControllerService(controllerService, processGroupId);
    }

    public void updateControllerService(ControllerServiceEntity controllerService) {
        this.nifiClient.updateControllerService(controllerService);
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

    public ProcessorsEntity getProcessors(String processGroupId) {
        return this.nifiClient.getProcessors(processGroupId);
    }

    public void startProcessGroup(ProcessGroupEntity processGroup) {
        ProcessorsEntity processors = this.nifiClient.getProcessors(processGroup.getId());

        processors.getProcessors()
            .forEach(processor -> {
                System.out.println(processor.getId());
                processor.getComponent().setName("Test");
                this.nifiClient.updateProcessor(processor);
            });
    }
}
