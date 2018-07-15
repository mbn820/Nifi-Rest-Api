package com.exist.nifirestapi.nifiremotesetup;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;

import com.exist.nifirestapi.builder.PortBuilder;
import com.exist.nifirestapi.builder.ProcessorBuilder;
import com.exist.nifirestapi.service.NifiService;
import com.exist.nifirestapi.util.PositionUtil;

import org.apache.nifi.web.api.dto.PositionDTO;
import org.apache.nifi.web.api.entity.ControllerServiceEntity;
import org.apache.nifi.web.api.entity.PortEntity;
import org.apache.nifi.web.api.entity.ProcessGroupEntity;
import org.apache.nifi.web.api.entity.ProcessorEntity;

public class HistoricalDataPGSetup {

    private NifiService nifiService;
    private ProcessGroupEntity locationKeysProcessGroup;
    private PortEntity locationKeysInputPort;
    private ProcessorEntity getHistoricalData;
    private ProcessorEntity splitHistoricalData;
    private ProcessorEntity extractFields;


    public HistoricalDataPGSetup() {}
    
    public HistoricalDataPGSetup(NifiService nifiService, 
                                 ProcessGroupEntity processGroup, 
                                 Map<String, ControllerServiceEntity> controllerServices) {

        this.nifiService = nifiService;
        this.locationKeysProcessGroup = processGroup;
    }

    public void setup() {
        addProcessors();
        connectProcessors();
    }

    public void addProcessors() {
        String processGroupId = this.locationKeysProcessGroup.getId();

        locationKeysInputPort = nifiService.addInputPort(createLocationKeyInputPort(), processGroupId);

        getHistoricalData   = nifiService.addProcessor(createGetHistoricalData(), processGroupId);
        splitHistoricalData = nifiService.addProcessor(createSplitHistoricalData(), processGroupId);
        extractFields       = nifiService.addProcessor(createExtractFields(), processGroupId);
    }

    public void connectProcessors() {
        String processGroupId = this.locationKeysProcessGroup.getId();

        nifiService.connectComponents(locationKeysInputPort, getHistoricalData, new ArrayList<String>(), processGroupId);
		nifiService.connectComponents(getHistoricalData, splitHistoricalData, Arrays.asList("Response"), processGroupId);
		nifiService.connectComponents(splitHistoricalData, extractFields, Arrays.asList("split"), processGroupId);
    }

    public PortEntity getInputPort() {
        return locationKeysInputPort;
    }

    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public PortEntity createLocationKeyInputPort() {
        return new PortBuilder()
            .name("LOCATION KEYS INPUT PORT")
            .type("INPUT_PORT")
            .position(new PositionDTO(1000.00, 1000.00))
            .build();
    }

    public ProcessorEntity createGetHistoricalData() {
        return new ProcessorBuilder()
            .name("Get historical data from accuweather api")
            .type("org.apache.nifi.processors.standard.InvokeHTTP")
            .position(PositionUtil.belowOf(locationKeysInputPort))
                .addConfigProperty("Remote URL", "https://dataservice.accuweather.com/currentconditions/v1/${locationKey}/historical/24?apikey=${apikey}&details=true")
                .addConfigProperty("Proxy Type", "http")
            .autoTerminateAt("Failure")
            .autoTerminateAt("No Retry")
            .autoTerminateAt("Original")
            .autoTerminateAt("Retry")
            .build();
    }

    public ProcessorEntity createSplitHistoricalData() {
        return new ProcessorBuilder()
            .name("Split into individual hourly data")
            .type("org.apache.nifi.processors.standard.SplitJson")
            .position(PositionUtil.belowOf(getHistoricalData))
                .addConfigProperty("JsonPath Expression", "$.*")
            .autoTerminateAt("failure")
            .autoTerminateAt("original")
            .build();
    }

    public ProcessorEntity createExtractFields() {
        return new ProcessorBuilder()
            .name("Extract desired fields")
            .type("org.apache.nifi.processors.standard.EvaluateJsonPath")
            .position(PositionUtil.belowOf(splitHistoricalData))
                .addConfigProperty("Destination", "flowfile-attribute")
                .addConfigProperty("date_time", "$.LocalObservationDateTime")
                .addConfigProperty("humidity", "$.RelativeHumidity")
                .addConfigProperty("sky_condition", "$.WeatherText")
                .addConfigProperty("temp", "$.Temperature.Metric.Value")
            .autoTerminateAt("failure")
            .autoTerminateAt("unmatched")
            .build();
    }

}