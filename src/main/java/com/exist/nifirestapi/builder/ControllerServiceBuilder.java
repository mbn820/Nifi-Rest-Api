package com.exist.nifirestapi.builder;

import org.apache.nifi.web.api.dto.ControllerServiceDTO;
import org.apache.nifi.web.api.dto.RevisionDTO;
import org.apache.nifi.web.api.entity.ControllerServiceEntity;

import java.util.HashMap;
import java.util.Map;

public class ControllerServiceBuilder {

    private String name;
    private String type;
    private String state;
    private Map<String, String> properties = new HashMap<>();

    public ControllerServiceBuilder name(String controllerName) {
        this.name = controllerName;
        return this;
    }

    public ControllerServiceBuilder type(String controllerType) {
        this.type = controllerType;
        return this;
    }

    public ControllerServiceBuilder state(String controllerState) {
        this.state = controllerState;
        return this;
    }

    public ControllerServiceBuilder addProperty(String property, String value) {
        this.properties.put(property, value);
        return this;
    }

    public ControllerServiceEntity build() {
        RevisionDTO revision = new RevisionDTO();
            revision.setVersion(0L);

        ControllerServiceDTO component = new ControllerServiceDTO();
            component.setName(this.name);
            component.setType(this.type);
            component.setState(this.state);
            component.setProperties(this.properties);

        ControllerServiceEntity controllerService = new ControllerServiceEntity();
            controllerService.setRevision(revision);
            controllerService.setComponent(component);

        return controllerService;
    }
}
