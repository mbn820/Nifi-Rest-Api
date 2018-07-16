package com.exist.nifirestapi.builder;

import org.apache.nifi.web.api.dto.FunnelDTO;
import org.apache.nifi.web.api.dto.PositionDTO;
import org.apache.nifi.web.api.dto.RevisionDTO;
import org.apache.nifi.web.api.entity.FunnelEntity;

public class FunnelBuilder {

    private PositionDTO position;

    public FunnelBuilder position(PositionDTO position) {
        this.position = position;

        return this;
    }

    public FunnelEntity build() {
        RevisionDTO revision = new RevisionDTO();
            revision.setVersion(0L);

        FunnelDTO component = new FunnelDTO();
            component.setPosition(this.position);

        FunnelEntity funnel = new FunnelEntity();
            funnel.setRevision(revision);
            funnel.setComponent(component);

        return funnel;
    }
}