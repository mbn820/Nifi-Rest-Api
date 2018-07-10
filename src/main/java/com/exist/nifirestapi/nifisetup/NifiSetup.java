package com.exist.nifirestapi.nifisetup;

import java.util.ArrayList;

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

			lKeyProcessGroupSetup.setup();
			hDataProcessGroupSetup.setup();
			fDataProcessGroupSetup.setup();

			nifiService.connectComponents(
				lKeyProcessGroupSetup.getOutputPort(), 
				hDataProcessGroupSetup.getInputPort(), 
				new ArrayList<String>(), 
				"root");

			nifiService.connectComponents(
				lKeyProcessGroupSetup.getOutputPort(), 
				fDataProcessGroupSetup.getInputPort(), 
				new ArrayList<String>(), 
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
	}





}