package com.exist.nifirestapi.nifiremotesetup;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import com.exist.nifirestapi.builder.FunnelBuilder;
import com.exist.nifirestapi.builder.PortBuilder;
import com.exist.nifirestapi.builder.ProcessGroupBuilder;
import com.exist.nifirestapi.builder.ProcessorBuilder;
import com.exist.nifirestapi.builder.RemoteProcessGroupBuilder;
import com.exist.nifirestapi.nifiremotesetup.port7070setup.PutSftpPGSetup;
import com.exist.nifirestapi.nifiremotesetup.port8080setup.ForecastDailyDataPGSetup;
import com.exist.nifirestapi.nifiremotesetup.port8080setup.ForecastHourlyPGSetup;
import com.exist.nifirestapi.nifiremotesetup.port8080setup.HistoricalDataPGSetup;
import com.exist.nifirestapi.nifiremotesetup.port8080setup.LocationKeysPGSetup;
import com.exist.nifirestapi.nifiremotesetup.port8080setup.PersistIntoDatabasePGSetup;
import com.exist.nifirestapi.service.NifiService;
import com.exist.nifirestapi.util.PositionUtil;

import org.apache.nifi.web.api.entity.ControllerServiceEntity;
import org.apache.nifi.web.api.entity.FunnelEntity;
import org.apache.nifi.web.api.entity.PortEntity;
import org.apache.nifi.web.api.entity.ProcessGroupEntity;
import org.apache.nifi.web.api.entity.ProcessorEntity;
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
    private FunnelEntity funnel; 
    private RemoteProcessGroupEntity port7070RemotePG;
    private PortEntity port8080InputPort;
    private ProcessGroupEntity persistIntoDatabase;

    // PORT 7070 COMPONENTS
    private Map<String, ControllerServiceEntity> port7070ControllerServices = new HashMap<>();
    private PortEntity port7070InputPort;
    private ProcessorEntity convertTemperature;
    private ProcessGroupEntity putSftp;
    private RemoteProcessGroupEntity port8080RemotePG;

    //SETUP
    private LocationKeysPGSetup locationKeysPGSetup;
    private ForecastDailyDataPGSetup forecastDailyDataPGSetup;
    private ForecastHourlyPGSetup forecastHourlyPGSetup;
    private HistoricalDataPGSetup historicalDataPGSetup;
    private PutSftpPGSetup putSftpPGSetup;
    private PersistIntoDatabasePGSetup persistIntoDatabasePGSetup;


    @Bean
    public CommandLineRunner run() {
        return args -> {

            createComponents8080();
            createComponents7070();

            setupProcessGroups();

            connectComponents8080();
            connectComponents7070();

            connectToRemotes();
            enableTransmission();

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

        funnel = nifiService8080.addFunnel(
            new FunnelBuilder()
                .position(PositionUtil.belowOf(fetchHistoricalData))
                .build(),

            "root"
        );

        port7070RemotePG = nifiService8080.addRemoteProcessGroup(
            new RemoteProcessGroupBuilder()
                .targetUri("http://localhost:7070/nifi")
                .position(PositionUtil.belowOf(funnel))
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

        convertTemperature = nifiService7070.addProcessor(
            new ProcessorBuilder()
                .name("CONVERT TEMPERATURE")
                .type("exist.processors.sample.TemperatureConvert")
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

    public void setupProcessGroups() {

        locationKeysPGSetup = 
            new LocationKeysPGSetup(nifiService8080, fetchLocationKeys, controllerServices);

        forecastDailyDataPGSetup = 
            new ForecastDailyDataPGSetup(nifiService8080, fetchForecastDailyData, controllerServices);

        forecastHourlyPGSetup = 
            new ForecastHourlyPGSetup(nifiService8080, fetchForecastHourlyData, controllerServices);

        historicalDataPGSetup = 
            new HistoricalDataPGSetup(nifiService8080, fetchHistoricalData, controllerServices);

        putSftpPGSetup = 
            new PutSftpPGSetup(nifiService7070, putSftp, controllerServices);

        persistIntoDatabasePGSetup = 
            new PersistIntoDatabasePGSetup(nifiService8080, persistIntoDatabase, controllerServices);

        locationKeysPGSetup.setup();
        forecastDailyDataPGSetup.setup();
        forecastHourlyPGSetup.setup();
        historicalDataPGSetup.setup();
        putSftpPGSetup.setup();
        persistIntoDatabasePGSetup.setup();

    }

    public void connectComponents8080() {

        nifiService8080.connectComponents(
            locationKeysPGSetup.getOutputPort(),
            historicalDataPGSetup.getInputPort(),
            Arrays.asList(),
            "root"
        );

        nifiService8080.connectComponents(
            locationKeysPGSetup.getOutputPort(),
            forecastDailyDataPGSetup.getInputPort(),
            Arrays.asList(),
            "root"
        );

        nifiService8080.connectComponents(
            locationKeysPGSetup.getOutputPort(),
            forecastHourlyPGSetup.getInputPort(),
            Arrays.asList(),
            "root"
        );

        nifiService8080.connectComponents(
            forecastDailyDataPGSetup.getOutputPort(),
            funnel,
            Arrays.asList(),
            "root"
        );

        nifiService8080.connectComponents(
            forecastHourlyPGSetup.getOutputPort(),
            funnel,
            Arrays.asList(),
            "root"
        );

        nifiService8080.connectComponents(
            historicalDataPGSetup.getOutputPort(),
            funnel,
            Arrays.asList(),
            "root"
        );

        nifiService8080.connectComponents(
            port8080InputPort,
            persistIntoDatabasePGSetup.getInputPort(),
            Arrays.asList(),
            "root"
        );

    }

    public void connectComponents7070() {

        nifiService7070.connectComponents(
            port7070InputPort,
            convertTemperature,
            Arrays.asList(),
            "root"
        );

        nifiService7070.connectComponents(
            convertTemperature,
            putSftpPGSetup.getInputPort(),
            Arrays.asList("CELSIUS", "FAHRENHEIT", "KELVIN"),
            "root"
        );

    }

    public void connectToRemotes() {

        nifiService8080.connectToRemoteProcessGroup(
            funnel,
            port7070RemotePG,
            port7070InputPort.getComponent().getName(),
            Arrays.asList(),
            "root"
        );

        nifiService7070.connectToRemoteProcessGroup(
            convertTemperature,
            port8080RemotePG,
            port8080InputPort.getComponent().getName(),
            Arrays.asList("CELSIUS", "FAHRENHEIT", "KELVIN"),
            "root"
        );

    }

    public void enableTransmission() {

        nifiService8080.enableRemoteProcessGroupTransmission(port7070RemotePG);
        nifiService7070.enableRemoteProcessGroupTransmission(port8080RemotePG);

    }
    
             

}