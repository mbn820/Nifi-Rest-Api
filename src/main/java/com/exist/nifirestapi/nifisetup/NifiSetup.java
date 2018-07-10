package com.exist.nifirestapi.nifisetup;

import java.util.Arrays;

import com.exist.nifirestapi.builder.ProcessGroupBuilder;
import com.exist.nifirestapi.service.NifiService;
import com.exist.nifirestapi.util.PositionUtil;

import org.apache.nifi.web.api.dto.PositionDTO;
import org.apache.nifi.web.api.entity.ProcessGroupEntity;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

@Component
public class NifiSetup {

    @Autowired
	private NifiService nifiService;

	private ProcessGroupEntity locationKeysProcessGroup;
	private ProcessGroupEntity historicalDataProcessGroup;
	private ProcessGroupEntity forecastDailyDataProcessGroup;
	private ProcessGroupEntity forecastHourlyDataProcessGroup;


    @Bean
    public CommandLineRunner run() {
        return args -> {

			createProcessGroups();

			LocationKeysProcessGroupSetup lKeyProcessGroupSetup =
				new LocationKeysProcessGroupSetup(nifiService, locationKeysProcessGroup);

			HistoricalDataProcessGroupSetup hDataProcessGroupSetup =
				new HistoricalDataProcessGroupSetup(nifiService, historicalDataProcessGroup);

			ForecastDataProcessGroupSetup fDataProcessGroupSetup =
				new ForecastDataProcessGroupSetup(nifiService, forecastDailyDataProcessGroup);

			ForecastHourlyProcessGroupSetup forecastHourlyProcessGroupSetup = 
			    new ForecastHourlyProcessGroupSetup(nifiService, forecastHourlyDataProcessGroup);

			lKeyProcessGroupSetup.setup();
			hDataProcessGroupSetup.setup();
			fDataProcessGroupSetup.setup();
			forecastHourlyProcessGroupSetup.setup();

			nifiService.connectComponents(
				lKeyProcessGroupSetup.getOutputPort(),
				hDataProcessGroupSetup.getInputPort(),
				Arrays.asList(),
				"root");

			nifiService.connectComponents(
				lKeyProcessGroupSetup.getOutputPort(),
				fDataProcessGroupSetup.getInputPort(), 
				Arrays.asList(),
				"root");

			nifiService.connectComponents(
				lKeyProcessGroupSetup.getOutputPort(),
				forecastHourlyProcessGroupSetup.getInputPort(), 
				Arrays.asList(),
				"root");

        };
	}

	public void createProcessGroups() {
		this.locationKeysProcessGroup = nifiService.addProcessGroup(
            new ProcessGroupBuilder()
                .name("GET LOCATION KEYS")
                .position(new PositionDTO(900.00, 900.00))
				.build(),

			"root"
        );

        this.historicalDataProcessGroup = nifiService.addProcessGroup(
            new ProcessGroupBuilder()
                .name("GET HISTORICAL DATA")
                .position(PositionUtil.rightOf(locationKeysProcessGroup))
                .build(),

			"root"
        );

        this.forecastDailyDataProcessGroup = nifiService.addProcessGroup(
            new ProcessGroupBuilder()
                .name("GET FORECAST DATA")
                .position(PositionUtil.leftOf(locationKeysProcessGroup))
                .build(),

			"root"
		);
		
		this.forecastHourlyDataProcessGroup = nifiService.addProcessGroup(
            new ProcessGroupBuilder()
                .name("GET FORECAST DATA")
                .position(PositionUtil.belowOf(locationKeysProcessGroup))
                .build(),

			"root"
        );
	}





}
