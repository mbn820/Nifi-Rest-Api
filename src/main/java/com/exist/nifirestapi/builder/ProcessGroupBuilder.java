package com.exist.nifirestapi.builder;

import org.apache.nifi.web.api.dto.PositionDTO;
import org.apache.nifi.web.api.dto.ProcessGroupDTO;
import org.apache.nifi.web.api.dto.RevisionDTO;
import org.apache.nifi.web.api.entity.ProcessGroupEntity;

public class ProcessGroupBuilder {

    private String name;
    private PositionDTO position;

    public ProcessGroupBuilder name(String processGroupName) {
        this.name = processGroupName;

        return this;
    }

    public ProcessGroupBuilder position(PositionDTO processGroupPosition) {
        this.position = processGroupPosition;

        return this;
    }

    public ProcessGroupEntity build() {
        RevisionDTO revision = new RevisionDTO();
            revision.setVersion(0L);

        ProcessGroupDTO component = new ProcessGroupDTO();
            component.setName(this.name);
            component.setPosition(this.position);

        ProcessGroupEntity processGroup = new ProcessGroupEntity();
            processGroup.setRevision(revision);
            processGroup.setComponent(component);

        return processGroup;
    }
}