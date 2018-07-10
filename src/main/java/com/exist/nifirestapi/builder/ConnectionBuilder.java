package com.exist.nifirestapi.builder;

import java.util.HashSet;
import java.util.Set;

import org.apache.nifi.web.api.dto.ComponentDTO;
import org.apache.nifi.web.api.dto.ConnectableDTO;
import org.apache.nifi.web.api.dto.ConnectionDTO;
import org.apache.nifi.web.api.dto.RevisionDTO;
import org.apache.nifi.web.api.entity.ComponentEntity;
import org.apache.nifi.web.api.entity.ConnectionEntity;
import org.apache.nifi.web.api.entity.Permissible;

public class ConnectionBuilder {

    private String sourceId;
    private String sourceGroupId;
    private String sourceType;

    private String destinationId;
    private String destinationGroupId;
    private String destinationType;

    private Set<String> connectionRelationships = new HashSet<>();

    public <T extends ComponentEntity & Permissible<? extends ComponentDTO>> ConnectionBuilder source(T sourceComponent) {
        this.sourceId = sourceComponent.getId();
        this.sourceGroupId = sourceComponent.getComponent().getParentGroupId();
        this.sourceType = "PROCESSOR";

        return this;
    }

    public <T extends ComponentEntity & Permissible<? extends ComponentDTO>> ConnectionBuilder destination(T destinationComponent) {
        this.destinationId = destinationComponent.getId();
        this.destinationGroupId = destinationComponent.getComponent().getParentGroupId();
        this.destinationType = "PROCESSOR";

        return this;
    }

    public ConnectionBuilder addConnectionRelationship(String relationship) {
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
