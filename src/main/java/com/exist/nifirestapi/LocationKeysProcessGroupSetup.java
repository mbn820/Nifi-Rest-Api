package com.exist.nifirestapi;

import java.util.Arrays;
import java.util.List;

import com.exist.nifirestapi.builder.PortBuilder;
import com.exist.nifirestapi.builder.ProcessorBuilder;
import com.exist.nifirestapi.service.NifiService;
import com.exist.nifirestapi.util.PositionUtil;

import org.apache.nifi.web.api.dto.PositionDTO;
import org.apache.nifi.web.api.entity.PortEntity;
import org.apache.nifi.web.api.entity.ProcessGroupEntity;
import org.apache.nifi.web.api.entity.ProcessorEntity;
import org.springframework.beans.factory.annotation.Autowired;

public class LocationKeysProcessGroupSetup {

    @Autowired
    private NifiService nifiService;

    private ProcessGroupEntity locationKeysProcessGroup;

    private ProcessorEntity loadCities;
    private ProcessorEntity assignTimeStamp;
    private ProcessorEntity splitCities;
    private ProcessorEntity extractCity;
    private ProcessorEntity getLocationKeys;
    private ProcessorEntity extractLocationKey;

    private PortEntity locationKeyOutputPort;

    public void setProcessGroup(ProcessGroupEntity processGroup) {
       
    }

    private void addProcessors() {
        String processGroupId = this.locationKeysProcessGroup.getId();

        loadCities         = nifiService.addProcessor(createLoadCities(), processGroupId);
        assignTimeStamp    = nifiService.addProcessor(createAssignTimeStamp(), processGroupId);
        splitCities        = nifiService.addProcessor(createSplitCities(), processGroupId);
        extractCity        = nifiService.addProcessor(createExtractCity(), processGroupId);
        getLocationKeys    = nifiService.addProcessor(createGetLocationKeys(), processGroupId);
        extractLocationKey = nifiService.addProcessor(createExtractLocationKeys(), processGroupId);
    }

    private void connectProcessors() {
        String processGroupId = this.locationKeysProcessGroup.getId();

        List<String> con1 = Arrays.asList("success");
        List<String> con2 = Arrays.asList("success");
        List<String> con3 = Arrays.asList("split");
        List<String> con4 = Arrays.asList("matched");
        List<String> con5 = Arrays.asList("Response");

        nifiService.connectProcessors(loadCities, assignTimeStamp, con1, processGroupId);
        nifiService.connectProcessors(assignTimeStamp, splitCities, con2, processGroupId);
        nifiService.connectProcessors(splitCities, extractCity, con3, processGroupId);
        nifiService.connectProcessors(extractCity, getLocationKeys, con4, processGroupId);
        nifiService.connectProcessors(getLocationKeys, extractLocationKey, con5, processGroupId);
    }

    public ProcessorEntity createLoadCities() {
        return new ProcessorBuilder()
            .name("Load Cities")
            .type("org.apache.nifi.processors.standard.GenerateFlowFile")
            .scheduling("1 day")
            .position(new PositionDTO(1000.00, 1000.00))
                .addConfigProperty("generate-ff-custom-text",
                    "[{ \"city\": \"Manila\" },"
                + "{ \"city\": \"Makati\" },"
                + "{ \"city\": \"Marikina\" },"
                + "{ \"city\": \"Pasig\" },"
                + "{ \"city\": \"Quezon City\" }]"
                )
            .build();
    }

    public ProcessorEntity createAssignTimeStamp() {
        return new ProcessorBuilder()
            .name("Assign Timestamp")
            .type("org.apache.nifi.processors.attributes.UpdateAttribute")
            .position(PositionUtil.belowOf(loadCities))
                .addConfigProperty("apiKey", "qQyAa99QwlKLMop22S9KgDgu5px1lHcT")
                .addConfigProperty("time_retrieved", "${now():format('yyyy-MM-dd HH:mm:ss')}")
            .build();
    }

    public ProcessorEntity createSplitCities() {
        return new ProcessorBuilder()
            .name("Split into individual cities")
            .type("org.apache.nifi.processors.standard.SplitJson")
            .position(PositionUtil.belowOf(assignTimeStamp))
                .addConfigProperty("JsonPath Expression", "$.*")
            .autoTerminateAt("failure")
            .autoTerminateAt("original")
            .build();
    }

    public ProcessorEntity createExtractCity() {
        return new ProcessorBuilder()
            .name("Extract city name to attribute")
            .type("org.apache.nifi.processors.standard.EvaluateJsonPath")
            .position(PositionUtil.belowOf(splitCities))
                .addConfigProperty("Destination", "flowfile-attribute")
                .addConfigProperty("city", "$.city")
            .autoTerminateAt("failure")
            .autoTerminateAt("unmatched")
            .build();
    }

    public ProcessorEntity createGetLocationKeys() {
        return new ProcessorBuilder()
            .name("Get location keys using accuweather locations api")
            .type("org.apache.nifi.processors.standard.InvokeHTTP")
            .position(PositionUtil.belowOf(extractCity))
                .addConfigProperty("Remote URL", "https://dataservice.accuweather.com/locations/v1/cities/ph/search?apikey=${apikey}&q=${city}")
                .addConfigProperty("Proxy Type", "http")
            .autoTerminateAt("Failure")
            .autoTerminateAt("No Retry")
            .autoTerminateAt("Original")
            .autoTerminateAt("Retry")
            .build();
    }

    public ProcessorEntity createExtractLocationKeys() {
        return new ProcessorBuilder()
            .name("Extract location key to attribute")
            .type("org.apache.nifi.processors.standard.EvaluateJsonPath")
            .position(PositionUtil.belowOf(getLocationKeys))
                .addConfigProperty("Destination", "flowfile-attribute")
                .addConfigProperty("city", "$[0].EnglishName")
                .addConfigProperty("locationKey", "$[0].Key")
            .autoTerminateAt("failure")
            .autoTerminateAt("matched")
            .autoTerminateAt("unmatched")
            .build();
    }

    public PortEntity createLocationKeysOutputPort() {
        return new PortBuilder()
            .name("LOCATION KEYS OUTPUT PORT")
            .type("OUTPUT PORT")
            .position(PositionUtil.belowOf(extractLocationKey))
            .build();
    }

    public void connect(ProcessorEntity source, ProcessorEntity destination, List<String> con) {
        
    }
}