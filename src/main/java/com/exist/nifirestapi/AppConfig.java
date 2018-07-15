package com.exist.nifirestapi;

import java.text.SimpleDateFormat;
import java.util.Date;

import com.exist.nifirestapi.client.NifiClient;
import com.exist.nifirestapi.service.NifiService;
import com.exist.nifirestapi.util.DateDeserializer;
import com.fasterxml.jackson.annotation.JsonInclude.Include;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.converter.json.Jackson2ObjectMapperBuilder;
import org.springframework.web.client.RestTemplate;

@Configuration
public class AppConfig {

    @Bean
	public RestTemplate restTemplate(RestTemplateBuilder builder) {
		return builder.build();
	}

	@Bean
	public Jackson2ObjectMapperBuilder jackson2ObjectMapperBuilder() {
		return new Jackson2ObjectMapperBuilder()
			.dateFormat(new SimpleDateFormat("HH:mm:ss zzz"))
			.deserializerByType(Date.class, new DateDeserializer())
			.serializationInclusion(Include.NON_NULL);
	}

	@Bean("nifiClient7070")
	public NifiClient nifiClient7070(RestTemplate restTemplate) {
		NifiClient nifiClient = new NifiClient("localhost", 7070, restTemplate);

		return nifiClient;
	}

	@Bean("nifiClient8080")
	public NifiClient nifiClient8080(RestTemplate restTemplate) {
		NifiClient nifiClient = new NifiClient("localhost", 8080, restTemplate);

		return nifiClient;
	}

	@Bean("nifiClient9090")
	public NifiClient nifiClient9090(RestTemplate restTemplate) {
		NifiClient nifiClient = new NifiClient("localhost", 9090, restTemplate);

		return nifiClient;
	}

	@Bean("nifiService7070")
	public NifiService nifiService7070(@Qualifier("nifiClient7070") NifiClient nifiClient) {
		return new NifiService(nifiClient);
	}

	@Bean("nifiService8080")
	public NifiService nifiService8080(@Qualifier("nifiClient8080") NifiClient nifiClient) {
		return new NifiService(nifiClient);
	}

	@Bean("nifiService9090")
	public NifiService nifiService9090(@Qualifier("nifiClient9090") NifiClient nifiClient) {
		return new NifiService(nifiClient);
	}

}
