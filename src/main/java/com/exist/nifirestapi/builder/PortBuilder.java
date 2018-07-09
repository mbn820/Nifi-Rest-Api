package com.exist.nifirestapi.builder;

import org.apache.nifi.web.api.dto.PortDTO;
import org.apache.nifi.web.api.dto.PositionDTO;
import org.apache.nifi.web.api.dto.RevisionDTO;
import org.apache.nifi.web.api.entity.PortEntity;

public class PortBuilder {

    private String name;
    private String type;
    private PositionDTO position;

    public PortBuilder name(String name) {
        this.name = name;

        return this;
    }

    public PortBuilder type(String type) {
        this.type = type; 

        return this;
    }

    public PortBuilder position(PositionDTO position) {
        this.position = position;
        
        return this;
    }

    public PortEntity build() {
        RevisionDTO revision = new RevisionDTO();
            revision.setVersion(0L);

        PortDTO component = new PortDTO();
            component.setName(this.name);
            component.setType(this.type);
            component.setPosition(this.position);

        PortEntity port = new PortEntity();
            port.setRevision(revision);
            port.setComponent(component);

        return port;
    }
}