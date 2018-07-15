package com.exist.nifirestapi.builder;

import org.apache.nifi.web.api.dto.PositionDTO;
import org.apache.nifi.web.api.dto.RemoteProcessGroupDTO;
import org.apache.nifi.web.api.dto.RevisionDTO;
import org.apache.nifi.web.api.entity.RemoteProcessGroupEntity;

public class RemoteProcessGroupBuilder {

    private String targetUri;
    private PositionDTO position;
    private boolean transmitting;

    public RemoteProcessGroupBuilder targetUri(String targetUri) {
        this.targetUri = targetUri;
        
        return this;
    }

    public RemoteProcessGroupBuilder position(PositionDTO position) {
        this.position = position;
        
        return this;
    }

    public RemoteProcessGroupBuilder transmitting(boolean transmitting) {
        this.transmitting = transmitting;
        
        return this;
    }

    public RemoteProcessGroupEntity build() {
        RevisionDTO revision = new RevisionDTO();
            revision.setVersion(0L);

        RemoteProcessGroupDTO component = new RemoteProcessGroupDTO();
            component.setTargetUri(this.targetUri);
            component.setPosition(this.position);
            component.setTransmitting(this.transmitting);

        RemoteProcessGroupEntity remoteProcessGroup = new RemoteProcessGroupEntity();
            remoteProcessGroup.setRevision(revision);
            remoteProcessGroup.setComponent(component);

        return remoteProcessGroup;
    }
}