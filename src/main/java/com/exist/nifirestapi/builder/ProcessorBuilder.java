package com.exist.nifirestapi.builder;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.nifi.web.api.dto.PositionDTO;
import org.apache.nifi.web.api.dto.ProcessorConfigDTO;
import org.apache.nifi.web.api.dto.ProcessorDTO;
import org.apache.nifi.web.api.dto.RevisionDTO;
import org.apache.nifi.web.api.entity.ProcessorEntity;

public class ProcessorBuilder {
    
    private String name;
    private String type;
    private String schedulingPeriod;
    private PositionDTO position;
    private Map<String, String> config = new HashMap<>();
    private Set<String> autoTerminatedRelationships = new HashSet<>();

    public ProcessorBuilder() {}

    public ProcessorBuilder type(String processorType) {
        this.type = processorType;

        return this;
    }

    public ProcessorBuilder name(String processorName) {
        this.name = processorName;

        return this;
    }

    public ProcessorBuilder position(PositionDTO processorPosition) {
        this.position = processorPosition;

        return this;
    }

    public ProcessorBuilder autoTerminateAt(String relationship) {
        this.autoTerminatedRelationships.add(relationship);

        return this;
    }

    public ProcessorBuilder config(Map<String, String> processorConfig) {
        this.config = processorConfig;

        return this;
    }

    public ProcessorBuilder addConfigProperty(String property, String value) {
        this.config.put(property, value);

        return this;
    }

    public ProcessorBuilder scheduling(String schedulingPeriod) {
        this.schedulingPeriod = schedulingPeriod;

        return this;
    }

    public ProcessorEntity build() {
        RevisionDTO revision = new RevisionDTO();
			revision.setVersion(0L);

        ProcessorConfigDTO processorConfig = new ProcessorConfigDTO();
            processorConfig.setSchedulingPeriod(this.schedulingPeriod);
            processorConfig.setProperties(this.config);
            processorConfig.setAutoTerminatedRelationships(this.autoTerminatedRelationships);

		ProcessorDTO component = new ProcessorDTO();
			component.setName(this.name);
			component.setType(this.type);
            component.setConfig(processorConfig);
            component.setPosition(this.position);

		ProcessorEntity processorEntity = new ProcessorEntity();
			processorEntity.setRevision(revision);
            processorEntity.setComponent(component);

		return processorEntity;
    }

}
