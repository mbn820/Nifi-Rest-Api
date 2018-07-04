package com.exist.nifirestapi;

import java.util.HashMap;
import java.util.List;

import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.web.client.RestTemplate;

import org.apache.nifi.processors.standard.*;
import org.apache.nifi.web.api.entity.ProcessorEntity;
import org.apache.nifi.web.api.dto.ProcessorDTO;
import org.apache.nifi.web.api.dto.ProcessorConfigDTO;
import org.apache.nifi.web.api.dto.PositionDTO;
import org.apache.nifi.web.api.dto.RevisionDTO;


@SpringBootApplication
public class NifiRestApiApplication {
	private String API_URL = "http://localhost:8080/nifi-api/process-groups/root/processors";
	private String TEST_URL = "http://localhost:7777/contentListener";

	public static void main(String[] args) {
		SpringApplication.run(NifiRestApiApplication.class, args);
	}

	@Bean
	public RestTemplate restTemplate(RestTemplateBuilder builder) {
		return builder.build();
	}

	@Bean
	public CommandLineRunner run(RestTemplate restTemplate) throws Exception {
		return args -> {
			RevisionDTO revision = new RevisionDTO();
				revision.setVersion(0L);

			ProcessorConfigDTO config = new ProcessorConfigDTO();
				// config.setProperties(new HashMap<String, String>() {{
				// 	put("generate-ff-custom-text", "TESTTSTTSTSTST");
				// }});

			ProcessorDTO component = new ProcessorDTO();
				component.setName("GENERATE");
				component.setType(EvaluateJsonPath.class.getName());
				component.setPosition(new PositionDTO(1_000.00, 1_000.00));
				component.setConfig(config);

			ProcessorEntity processorEntity = new ProcessorEntity();
				processorEntity.setRevision(revision);
				processorEntity.setComponent(component);

			restTemplate.postForObject(TEST_URL, processorEntity, String.class);

			ProcessorEntity entity = createNewProcessor(
				AttributesToJSON.class,
				"ATTR",
				new PositionDTO(1_000.00, 1_200.00)
			);

			ProcessorEntity entity2 = createNewProcessor(
				AttributesToJSON.class,
				"ATTR",
				belowOf(entity)
			);

			restTemplate.postForObject(API_URL, entity, String.class);
			restTemplate.postForObject(API_URL, entity2, String.class);

		};
	}

	public ProcessorEntity createNewProcessor(Class<?> processorClass, String name, PositionDTO position) {
		RevisionDTO revision = new RevisionDTO();
			revision.setVersion(0L);

		ProcessorDTO component = new ProcessorDTO();
			component.setName(name);
			component.setType(processorClass.getName());
			component.setPosition(position);

		ProcessorEntity processorEntity = new ProcessorEntity();
			processorEntity.setRevision(revision);
			processorEntity.setComponent(component);

		return processorEntity;
	}

	public PositionDTO belowOf(ProcessorEntity processorEntity) {
		double referenceXCoordinate = processorEntity.getComponent().getPosition().getX();
		double referenceYCoordinate = processorEntity.getComponent().getPosition().getY();

		return new PositionDTO(referenceXCoordinate, referenceYCoordinate + 200.00);
	}

	public PositionDTO aboveOf(ProcessorEntity processorEntity) {
		double referenceXCoordinate = processorEntity.getComponent().getPosition().getX();
		double referenceYCoordinate = processorEntity.getComponent().getPosition().getY();

		return new PositionDTO(referenceXCoordinate, referenceYCoordinate - 200.00);
	}
}
