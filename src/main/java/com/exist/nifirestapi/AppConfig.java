package com.exist.nifirestapi;

import com.exist.nifirestapi.client.NifiClient;
import java.text.SimpleDateFormat;

import com.fasterxml.jackson.annotation.JsonInclude.Include;

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
		Jackson2ObjectMapperBuilder jackson2ObjectMapperBuilder = new Jackson2ObjectMapperBuilder()
            .dateFormat(new SimpleDateFormat("HH:mm:ss zzz"))
		    .serializationInclusion(Include.NON_NULL);

		return jackson2ObjectMapperBuilder;
	}

	@Bean
	public NifiClient nifiClient(RestTemplate restTemplate) {
		NifiClient nifiClient = new NifiClient(restTemplate);

		return nifiClient;
	}

}
