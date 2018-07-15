package com.exist.nifirestapi.nifiremotesetup;

import java.util.HashMap;
import java.util.Map;

import com.exist.nifirestapi.builder.PortBuilder;
import com.exist.nifirestapi.builder.ProcessGroupBuilder;
import com.exist.nifirestapi.builder.RemoteProcessGroupBuilder;
import com.exist.nifirestapi.service.NifiService;
import com.exist.nifirestapi.util.PositionUtil;

import org.apache.nifi.web.api.entity.ControllerServiceEntity;
import org.apache.nifi.web.api.entity.PortEntity;
import org.apache.nifi.web.api.entity.ProcessGroupEntity;
import org.apache.nifi.web.api.entity.RemoteProcessGroupEntity;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.CommandLineRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

@Component
public class NifiActivityRemoteSetup {

    @Autowired
    @Qualifier("nifiService7070")
    private NifiService nifiService7070;

    @Autowired
    @Qualifier("nifiService8080")
    private NifiService nifiService8080;

    @Autowired
    @Qualifier("nifiService9090")
    private NifiService nifiService9090;

    // CONTROLLER SERVICES
    private Map<String, ControllerServiceEntity> controllerServices = new HashMap<>();

    // PORT 8080 COMPONENTS
    private ProcessGroupEntity fetchLocationKeys;
    private ProcessGroupEntity fetchHistoricalData;
    private ProcessGroupEntity fetchForecastDailyData;
    private ProcessGroupEntity fetchForecastHourlyData;   
    private RemoteProcessGroupEntity port7070RemotePG;
    private PortEntity port8080InputPort;
    private ProcessGroupEntity persistIntoDatabase;

    // PORT 7070 COMPONENTS
    private PortEntity port7070InputPort;
    private ProcessGroupEntity convertTemperature;
    private ProcessGroupEntity putSftp;
    private RemoteProcessGroupEntity port8080RemotePG;


    @Bean
    public CommandLineRunner run() {
        return args -> {

            createComponents8080();
            createComponents7070();

            LocationKeysPGSetup locationKeysPGSetup = 
                new LocationKeysPGSetup(nifiService8080, fetchLocationKeys, controllerServices);

            ForecastDailyDataPGSetup forecastDailyDataPGSetup = 
                new ForecastDailyDataPGSetup(nifiService8080, fetchForecastDailyData, controllerServices);

            ForecastHourlyPGSetup forecastHourlyPGSetup = 
                new ForecastHourlyPGSetup(nifiService8080, fetchForecastHourlyData, controllerServices);

            HistoricalDataPGSetup historicalDataPGSetup = 
                new HistoricalDataPGSetup(nifiService8080, fetchHistoricalData, controllerServices);

            locationKeysPGSetup.setup();
            forecastDailyDataPGSetup.setup();
            forecastHourlyPGSetup.setup();
            historicalDataPGSetup.setup();
        };
    }

    public void createControllerServices() {
        
    }

    public void createComponents8080() {

        fetchLocationKeys = nifiService8080.addProcessGroup(
            new ProcessGroupBuilder()
                .name("Fetch Location Keys")
                .position(PositionUtil.startingPosition())
                .build(),

            "root"
        );

        fetchHistoricalData = nifiService8080.addProcessGroup(
            new ProcessGroupBuilder()
                .name("Fetch Historical Data")
                .position(PositionUtil.belowOf(fetchLocationKeys))
                .build(),

            "root"
        );

        fetchForecastDailyData = nifiService8080.addProcessGroup(
            new ProcessGroupBuilder()
                .name("Fetch Forecast Daily Data")
                .position(PositionUtil.leftOf(fetchHistoricalData))
                .build(),

            "root"
        );

        fetchForecastHourlyData = nifiService8080.addProcessGroup(
            new ProcessGroupBuilder()
                .name("Fetch Forecast Hourly Data")
                .position(PositionUtil.rightOf(fetchHistoricalData))
                .build(),

            "root"
        );

        port7070RemotePG = nifiService8080.addRemoteProcessGroup(
            new RemoteProcessGroupBuilder()
                .targetUri("http://localhost:7070/nifi")
                .position(PositionUtil.belowOf(fetchHistoricalData))
                .build(),

            "root"
        );

        port8080InputPort = nifiService8080.addInputPort(
            new PortBuilder()
                .name("PORT 8080 Input Port")
                .type("INPUT_PORT")
                .position(PositionUtil.belowOf(port7070RemotePG))
                .build(),

            "root"
        );

        persistIntoDatabase = nifiService8080.addProcessGroup(
            new ProcessGroupBuilder()
                .name("Persist Into Database")
                .position(PositionUtil.belowOf(port8080InputPort))
                .build(),

            "root"
        );

    }

    public void createComponents7070() {

        port7070InputPort = nifiService7070.addInputPort(
            new PortBuilder()
                .name("PORT 7070 Input Port")
                .type("INPUT_PORT")
                .position(PositionUtil.startingPosition())
                .build(),

            "root"
        );

        convertTemperature = nifiService7070.addProcessGroup(
            new ProcessGroupBuilder()
                .name("Convert Temperature")
                .position(PositionUtil.belowOf(port7070InputPort))
                .build(),

            "root"            
        );

        putSftp = nifiService7070.addProcessGroup(
            new ProcessGroupBuilder()
                .name("Put SFTP")
                .position(PositionUtil.rightOf(convertTemperature))
                .build(),

            "root"
        );

        port8080RemotePG = nifiService7070.addRemoteProcessGroup(
            new RemoteProcessGroupBuilder()
                .targetUri("http://localhost:8080/nifi")
                .position(PositionUtil.leftOf(convertTemperature))
                .build(),

            "root"
        );

    }
    
             

}