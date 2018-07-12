package com.exist.nifirestapi.builder;

import java.util.HashSet;
import java.util.Set;

import org.apache.nifi.web.api.dto.ComponentDTO;
import org.apache.nifi.web.api.dto.ConnectableDTO;
import org.apache.nifi.web.api.dto.ConnectionDTO;
import org.apache.nifi.web.api.dto.RevisionDTO;
import org.apache.nifi.web.api.entity.ConnectionEntity;
import org.apache.nifi.web.api.entity.Permissible;
import org.apache.nifi.web.api.entity.PortEntity;
import org.apache.nifi.web.api.entity.ProcessorEntity;

public class ConnectionBuilder2 {

    private String sourceId;
    private String sourceGroupId;
    private String sourceType = "PROCESSOR";

    private String destinationId;
    private String destinationGroupId;
    private String destinationType = "PROCESSOR";

    private Set<String> connectionRelationships = new HashSet<>();

    public <T extends Permissible<? extends ComponentDTO>> ConnectionBuilder2 source(T sourceComponent) {
        this.sourceId = sourceComponent.getComponent().getId();
        this.sourceGroupId = sourceComponent.getComponent().getParentGroupId();

        if (sourceComponent instanceof ProcessorEntity) {
            this.sourceType = "PROCESSOR";
        }

        if (sourceComponent instanceof PortEntity) {
            this.sourceType = ((PortEntity) sourceComponent).getPortType();
        }

        return this;
    }

    public <T extends Permissible<? extends ComponentDTO>> ConnectionBuilder2 destination(T destinationComponent) {
        this.destinationId = destinationComponent.getComponent().getId();
        this.destinationGroupId = destinationComponent.getComponent().getParentGroupId();

        if (destinationComponent instanceof ProcessorEntity) {
            this.sourceType = "PROCESSOR";
        }

        if (destinationComponent instanceof PortEntity) {
            this.sourceType = ((PortEntity) destinationComponent).getPortType();
        }

        return this;
    }

    public ConnectionBuilder2 addConnectionRelationship(String relationship) {
        this.connectionRelationships.add(relationship);

        return this;
    }

    public ConnectionEntity build() {
        RevisionDTO revision = new RevisionDTO();
			revision.setVersion(0L);

		ConnectableDTO source = new ConnectableDTO();
			source.setId(this.sourceId);
			source.setGroupId(this.sourceGroupId);
			source.setType(this.sourceType);

		ConnectableDTO destination = new ConnectableDTO();
			destination.setId(this.destinationId);
			destination.setGroupId(this.destinationGroupId);
			destination.setType(this.destinationType);

		ConnectionDTO component = new ConnectionDTO();
			component.setSource(source);
			component.setDestination(destination);
			component.setSelectedRelationships(this.connectionRelationships);

		ConnectionEntity connection = new ConnectionEntity();
			connection.setRevision(revision);
			connection.setComponent(component);

        return connection;
    }
}
