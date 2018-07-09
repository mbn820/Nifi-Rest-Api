package com.exist.nifirestapi.builder;

import java.util.HashSet;
import java.util.Set;

import org.apache.nifi.web.api.dto.ConnectableDTO;
import org.apache.nifi.web.api.dto.ConnectionDTO;
import org.apache.nifi.web.api.dto.RevisionDTO;
import org.apache.nifi.web.api.entity.ConnectionEntity;
import org.apache.nifi.web.api.entity.ProcessorEntity;

public class ConnectionBuilder {

    private String sourceId;
    private String sourceGroupId;
    private String destinationId;
    private String destinationGroupId;
    private Set<String> connectionRelationships = new HashSet<>();

    public ConnectionBuilder source(ProcessorEntity source) {
        this.sourceId = source.getId();
        this.sourceGroupId = source.getComponent().getParentGroupId();

        return this;
    }

    public ConnectionBuilder destination(ProcessorEntity destination) {
        this.destinationId = destination.getId();
        this.destinationGroupId = destination.getComponent().getParentGroupId();

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
			source.setType("PROCESSOR");

		ConnectableDTO destination = new ConnectableDTO();
			destination.setId(this.destinationId);
			destination.setGroupId(this.destinationGroupId);
			destination.setType("PROCESSOR");

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
