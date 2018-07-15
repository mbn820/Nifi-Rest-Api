package com.exist.nifirestapi.util;

import java.util.HashMap;
import java.util.Map;

import org.apache.nifi.web.api.entity.ComponentEntity;
import org.apache.nifi.web.api.dto.PositionDTO;

public class PositionUtil {

    public static PositionDTO template(ComponentEntity componentEntity, double xDistance, double yDistance) {
        PositionDTO referencePosition = componentEntity.getPosition();
        double xCoordinate = referencePosition.getX();
        double yCoordinate = referencePosition.getY();

		return new PositionDTO(xCoordinate + xDistance, yCoordinate + yDistance);
	}

    public static PositionDTO belowOf(ComponentEntity componentEntity) {
		return template(componentEntity, 0.00, 200.00);
	}

	public static PositionDTO aboveOf(ComponentEntity componentEntity) {
		return template(componentEntity, 0.00, - 200.00);
	}

    public static PositionDTO leftOf(ComponentEntity componentEntity) {
		return template(componentEntity,- 500.00, 0.00);
	}

	public static PositionDTO rightOf(ComponentEntity componentEntity) {
		return template(componentEntity, 500.00, 0.00);
	}

	public static PositionDTO startingPosition() {
		return new PositionDTO(1000.00, 1000.00);
	}



}
