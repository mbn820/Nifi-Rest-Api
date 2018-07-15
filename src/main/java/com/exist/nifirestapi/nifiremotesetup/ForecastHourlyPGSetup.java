package com.exist.nifirestapi.nifiremotesetup;

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

public class ForecastHourlyPGSetup {

    private NifiService nifiService;
    private PortEntity locationKeysInputPort;
    private ProcessGroupEntity locationKeysProcessGroup;
    private ProcessorEntity getForecastHourlyData;
    private ProcessorEntity splitForecastHourlyData;
    private ProcessorEntity extractFields;


    public ForecastHourlyPGSetup() {}

    public ForecastHourlyPGSetup(NifiService nifiService, 
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

        getForecastHourlyData   = nifiService.addProcessor(createGetForecastHourlyData(), processGroupId);
        splitForecastHourlyData = nifiService.addProcessor(createSplitForecastHourlyData(), processGroupId);
        extractFields           = nifiService.addProcessor(createExtractFields(), processGroupId);
    }

    public void connectProcessors() {
        String processGroupId = this.locationKeysProcessGroup.getId();

        nifiService.connectComponents(locationKeysInputPort, getForecastHourlyData, Arrays.asList(), processGroupId);
		nifiService.connectComponents(getForecastHourlyData, splitForecastHourlyData, Arrays.asList("Response"), processGroupId);
		nifiService.connectComponents(splitForecastHourlyData, extractFields, Arrays.asList("split"), processGroupId);
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

    public ProcessorEntity createGetForecastHourlyData() {
        return new ProcessorBuilder()
            .name("Get forecast data from accuweather api")
            .type("org.apache.nifi.processors.standard.InvokeHTTP")
            .position(PositionUtil.belowOf(locationKeysInputPort))
                .addConfigProperty("Remote URL", "https://dataservice.accuweather.com/forecasts/v1/hourly/12hour/${locationKey}?apikey=${apikey}&metric=true&details=true")
                .addConfigProperty("Proxy Type", "http")
            .autoTerminateAt("Failure")
            .autoTerminateAt("No Retry")
            .autoTerminateAt("Original")
            .autoTerminateAt("Retry")
            .build();
    }

    public ProcessorEntity createSplitForecastHourlyData() {
        return new ProcessorBuilder()
            .name("Split into individual hourly data")
            .type("org.apache.nifi.processors.standard.SplitJson")
            .position(PositionUtil.belowOf(getForecastHourlyData))
                .addConfigProperty("JsonPath Expression", "$.*")
            .autoTerminateAt("failure")
            .autoTerminateAt("original")
            .build();
    }

    public ProcessorEntity createExtractFields() {
        return new ProcessorBuilder()
            .name("Extract desired fields")
            .type("org.apache.nifi.processors.standard.EvaluateJsonPath")
            .position(PositionUtil.belowOf(splitForecastHourlyData))
                .addConfigProperty("Destination", "flowfile-attribute")
                .addConfigProperty("date_time", "$.DateTime")
                .addConfigProperty("humidity", "$.RelativeHumidity")
                .addConfigProperty("sky_condition", "$.IconPhrase")
                .addConfigProperty("temp", "$.Temperature.Value")
            .autoTerminateAt("failure")
            .autoTerminateAt("unmatched")
            .build();
    }

}
