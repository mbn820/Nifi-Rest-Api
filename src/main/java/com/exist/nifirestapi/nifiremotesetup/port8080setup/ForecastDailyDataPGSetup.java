package com.exist.nifirestapi.nifiremotesetup.port8080setup;

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

public class ForecastDailyDataPGSetup {

    private NifiService nifiService;
    private ProcessGroupEntity locationKeysProcessGroup;
    private PortEntity locationKeysInputPort;
    private PortEntity forecastDailyDataOutputPort;
    private ProcessorEntity getForecastDailyData;
    private ProcessorEntity splitForecastDailyData;
    private ProcessorEntity extractFields;
    private ProcessorEntity addAttributes;

    public ForecastDailyDataPGSetup() {}

    public ForecastDailyDataPGSetup(NifiService nifiService, 
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

        getForecastDailyData   = nifiService.addProcessor(createGetForecastDailyData(), processGroupId);
        splitForecastDailyData = nifiService.addProcessor(createSplitForecastDailyData(), processGroupId);
        extractFields          = nifiService.addProcessor(createExtractFields(), processGroupId);
        addAttributes          = nifiService.addProcessor(createAddAttributes(), processGroupId);

        forecastDailyDataOutputPort = nifiService.addOutputPort(createForecastDailyDataOutputPort(), processGroupId);
    }

    public void connectProcessors() {
        String processGroupId = this.locationKeysProcessGroup.getId();

        nifiService.connectComponents(locationKeysInputPort, getForecastDailyData, Arrays.asList(), processGroupId);
		nifiService.connectComponents(getForecastDailyData, splitForecastDailyData, Arrays.asList("Response"), processGroupId);
        nifiService.connectComponents(splitForecastDailyData, extractFields, Arrays.asList("split"), processGroupId);
        nifiService.connectComponents(extractFields, addAttributes, Arrays.asList("matched"), processGroupId);
        nifiService.connectComponents(addAttributes, forecastDailyDataOutputPort, Arrays.asList("success"), processGroupId);
    }

    public PortEntity getInputPort() {
        return locationKeysInputPort;
    }

    public PortEntity getOutputPort() {
        return forecastDailyDataOutputPort;
    }

    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public PortEntity createLocationKeyInputPort() {
        return new PortBuilder()
            .name("LOCATION KEYS INPUT PORT")
            .type("INPUT_PORT")
            .position(new PositionDTO(1000.00, 1000.00))
            .build();
    }

    public ProcessorEntity createGetForecastDailyData() {
        return new ProcessorBuilder()
            .name("Get forecast data from accuweather api")
            .type("org.apache.nifi.processors.standard.InvokeHTTP")
            .position(PositionUtil.belowOf(locationKeysInputPort))
                .addConfigProperty("Remote URL", "https://dataservice.accuweather.com/forecasts/v1/daily/5day/${locationKey}?apikey=${apikey}&metric=true")
                .addConfigProperty("Proxy Type", "http")
            .autoTerminateAt("Failure")
            .autoTerminateAt("No Retry")
            .autoTerminateAt("Original")
            .autoTerminateAt("Retry")
            .build();
    }

    public ProcessorEntity createSplitForecastDailyData() {
        return new ProcessorBuilder()
            .name("Split into individual daily data")
            .type("org.apache.nifi.processors.standard.SplitJson")
            .position(PositionUtil.belowOf(getForecastDailyData))
                .addConfigProperty("JsonPath Expression", "$.DailyForecasts")
            .autoTerminateAt("failure")
            .autoTerminateAt("original")
            .build();
    }

    public ProcessorEntity createExtractFields() {
        return new ProcessorBuilder()
            .name("Extract desired fields")
            .type("org.apache.nifi.processors.standard.EvaluateJsonPath")
            .position(PositionUtil.belowOf(splitForecastDailyData))
                .addConfigProperty("Destination", "flowfile-attribute")
                .addConfigProperty("date", "$.Date")
                .addConfigProperty("day_condition", "$.Day.IconPhrase")
                .addConfigProperty("night_condition", "$.Night.IconPhrase")
                .addConfigProperty("temp_max", "$.Temperature.Maximum.Value")
                .addConfigProperty("temp_min", "$.Temperature.Minimum.Value")
            .autoTerminateAt("failure")
            .autoTerminateAt("unmatched")
            .build();
    }

    public ProcessorEntity createAddAttributes() {
        return new ProcessorBuilder()
            .name("Add Attributes")
            .type("org.apache.nifi.processors.attributes.UpdateAttribute")
            .position(PositionUtil.belowOf(extractFields))
                .addConfigProperty("weatherDataType", "forecast_daily")
                .addConfigProperty("data_type", "daily")
            .build();
    }

    public PortEntity createForecastDailyDataOutputPort() {
        return new PortBuilder()
            .name("OUTGOING FORECAST DAILY DATA")
            .type("OUTPUT_PORT")
            .position(PositionUtil.belowOf(addAttributes))
            .build();
    }

}
