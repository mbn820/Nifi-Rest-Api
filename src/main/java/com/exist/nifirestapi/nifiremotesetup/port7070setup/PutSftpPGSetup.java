package com.exist.nifirestapi.nifiremotesetup.port7070setup;

import java.util.Arrays;
import java.util.Map;

import com.exist.nifirestapi.builder.ControllerServiceBuilder;
import com.exist.nifirestapi.builder.PortBuilder;
import com.exist.nifirestapi.builder.ProcessorBuilder;
import com.exist.nifirestapi.service.NifiService;
import com.exist.nifirestapi.util.PositionUtil;

import org.apache.nifi.web.api.entity.ControllerServiceEntity;
import org.apache.nifi.web.api.entity.PortEntity;
import org.apache.nifi.web.api.entity.ProcessGroupEntity;
import org.apache.nifi.web.api.entity.ProcessorEntity;

public class PutSftpPGSetup {

    private NifiService nifiService;
    private ProcessGroupEntity putSftpProcessGroup;
    private PortEntity putSftpPGInputPort;
    private ProcessorEntity routeOnAttribute;
    private ProcessorEntity transformToCsvDailyFormat;
    private ProcessorEntity transformToCsvHourlyFormat;
    private ProcessorEntity mergeRecordsDailyFormat;
    private ProcessorEntity mergeRecordsHourlyFormat;
    private ProcessorEntity assignFilename;
    private ProcessorEntity putSftp;

    private ControllerServiceEntity csvReaderHourlyData;
    private ControllerServiceEntity csvReaderDailyData;
    private ControllerServiceEntity csvWriterWeatherData;


    public PutSftpPGSetup() {}

    public PutSftpPGSetup(NifiService nifiService, 
                          ProcessGroupEntity processGroup,
                          Map<String, ControllerServiceEntity> controllerServices) {
        
        this.nifiService = nifiService;
        this.putSftpProcessGroup = processGroup;
    }

    public void setup() {
        createControllerServices();
        enableControllerServices();
        addProcessors();
        connectProcessors();
    }

    public void createControllerServices() {
        String hourlyDataSchema = 
			"{\"name\": \"historical\","
			+	"\"namespace\": \"nifi\","
			+	"\"type\": \"record\","
			+	"\"fields\": ["
			+		"{ \"name\": \"city\", \"type\": \"string\" },"
			+       "{ \"name\": \"locationKey\", \"type\": \"string\" },"
			+		"{ \"name\": \"date_time\", \"type\": \"string\" },"
			+		"{ \"name\": \"temp\", \"type\": \"double\" },"
			+		"{ \"name\": \"humidity\", \"type\": \"double\" },"
			+		"{ \"name\": \"sky_condition\", \"type\": \"string\" }] }";

		String dailyDataSchema = 
			"{\"name\": \"forecast\","
			+	"\"namespace\": \"nifi\","
			+	"\"type\": \"record\","
			+	"\"fields\": ["
			+		"{ \"name\": \"city\", \"type\": \"string\" },"
			+       "{ \"name\": \"locationKey\", \"type\": \"string\" },"
			+		"{ \"name\": \"date\", \"type\": \"string\" },"
			+		"{ \"name\": \"temp_min\", \"type\": \"double\" },"
			+		"{ \"name\": \"temp_max\", \"type\": \"double\" },"
			+		"{ \"name\": \"day_condition\", \"type\": \"string\" },"
			+		"{ \"name\": \"night_condition\", \"type\": \"string\" },"
			+		"{ \"name\": \"time_retrieved\", \"type\": \"string\" }] }";

		csvReaderHourlyData = nifiService.addControllerService(
			new ControllerServiceBuilder()
				.name("hourlyDataReader")
				.type("org.apache.nifi.csv.CSVReader")
					.addProperty("schema-access-strategy", "schema-text-property")
					.addProperty("schema-text", hourlyDataSchema)
				.state("DISABLED")
				.build(),

            putSftpProcessGroup.getId()
		);

		csvReaderDailyData = nifiService.addControllerService(
			new ControllerServiceBuilder()
				.name("dailyDataReader")
				.type("org.apache.nifi.csv.CSVReader")
					.addProperty("schema-access-strategy", "schema-text-property")
					.addProperty("schema-text", dailyDataSchema)
				.state("DISABLED")
				.build(),

            putSftpProcessGroup.getId()
		);

		csvWriterWeatherData = nifiService.addControllerService(
			new ControllerServiceBuilder()
				.name("weatherDataWriter")
				.type("org.apache.nifi.csv.CSVRecordSetWriter")
					.addProperty("Schema Write Strategy", "no-schema")
				.state("DISABLED")
				.build(),

            putSftpProcessGroup.getId()
		);
    }

    public void enableControllerServices() {
        nifiService.enableControllerService(csvReaderDailyData);
        nifiService.enableControllerService(csvReaderHourlyData);
        nifiService.enableControllerService(csvWriterWeatherData);
    }

    public void addProcessors() {
        String processGroupId = this.putSftpProcessGroup.getId();

        putSftpPGInputPort = nifiService.addInputPort(createPutSftpInputPort(), processGroupId);

        routeOnAttribute           = nifiService.addProcessor(createRouteOnAttribute(), processGroupId);
        transformToCsvDailyFormat  = nifiService.addProcessor(createTransformToCsvDailyFormat(), processGroupId);
        transformToCsvHourlyFormat = nifiService.addProcessor(createTransformToCsvHourlyFormat(), processGroupId);
        mergeRecordsDailyFormat    = nifiService.addProcessor(createMergeRecordsDailyFormat(), processGroupId);
        mergeRecordsHourlyFormat   = nifiService.addProcessor(createMergeRecordsHourlyFormat(), processGroupId);
        assignFilename             = nifiService.addProcessor(createAssignFilename(), processGroupId);
        putSftp                    = nifiService.addProcessor(createPutSftp(), processGroupId);

    }

    public void connectProcessors() {
        String processGroupId = this.putSftpProcessGroup.getId();

        nifiService.connectComponents(putSftpPGInputPort, routeOnAttribute, Arrays.asList(), processGroupId);
        nifiService.connectComponents(routeOnAttribute, transformToCsvDailyFormat, Arrays.asList("Daily"), processGroupId);
        nifiService.connectComponents(routeOnAttribute, transformToCsvHourlyFormat, Arrays.asList("Hourly"), processGroupId);
        nifiService.connectComponents(transformToCsvDailyFormat, mergeRecordsDailyFormat, Arrays.asList("success"), processGroupId);
        nifiService.connectComponents(transformToCsvHourlyFormat, mergeRecordsHourlyFormat, Arrays.asList("success"), processGroupId);
        nifiService.connectComponents(mergeRecordsDailyFormat, assignFilename, Arrays.asList("merged"), processGroupId);
        nifiService.connectComponents(mergeRecordsHourlyFormat, assignFilename, Arrays.asList("merged"), processGroupId);
        nifiService.connectComponents(assignFilename, putSftp, Arrays.asList("success"), processGroupId);
    }

    public PortEntity getInputPort() {
        return putSftpPGInputPort;
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////

    public PortEntity createPutSftpInputPort() {
        return new PortBuilder()
            .name("INCOMING ACCUWEATHER DATA")
            .type("INPUT_PORT")
            .position(PositionUtil.startingPosition())
            .build();
    }

    public ProcessorEntity createRouteOnAttribute() {
        return new ProcessorBuilder()
            .name("Route Based on data type")
            .type("org.apache.nifi.processors.standard.RouteOnAttribute")
            .position(PositionUtil.belowOf(putSftpPGInputPort))
                .addConfigProperty("Daily", "${data_type:equals('daily')}")
                .addConfigProperty("Hourly", "${data_type:equals('hourly')}")
            .autoTerminateAt("unmatched")
            .build();
    }

    public ProcessorEntity createTransformToCsvDailyFormat() {
        return new ProcessorBuilder()
            .name("Transform into csv format (Daily Format)")
            .type("org.apache.nifi.processors.standard.ReplaceText")
            .position(PositionUtil.lowerLeftOf(routeOnAttribute))
                .addConfigProperty("Replacement Value", "${city},${locationKey},${date},${temp_min},${temp_max},${day_condition},${night_condition},${time_retrieved}")
            .autoTerminateAt("failure")
            .build();
    }

    public ProcessorEntity createTransformToCsvHourlyFormat() {
        return new ProcessorBuilder()
            .name("Transform into csv format (Hourly Format)")
            .type("org.apache.nifi.processors.standard.ReplaceText")
            .position(PositionUtil.lowerRightOf(routeOnAttribute))
                .addConfigProperty("Replacement Value", "${city},${locationKey},${date_time},${temp},${humidity},${sky_condition},${time_retrieved}")
            .autoTerminateAt("failure")
            .build();
    }

    public ProcessorEntity createMergeRecordsDailyFormat() {
        return new ProcessorBuilder()
            .name("Merge Records (Daily Format)")
            .type("org.apache.nifi.processors.standard.MergeRecord")
            .position(PositionUtil.belowOf(transformToCsvDailyFormat))
                .addConfigProperty("record-reader", csvReaderDailyData.getId())
                .addConfigProperty("record-writer", csvWriterWeatherData.getId())
                .addConfigProperty("merge-strategy", "Defragment")
                .addConfigProperty("max-bin-age", "1 day")
            .autoTerminateAt("failure")
            .autoTerminateAt("original")
            .build();
    }

    public ProcessorEntity createMergeRecordsHourlyFormat() {
        return new ProcessorBuilder()
            .name("Merge Records (Hourly Format)")
            .type("org.apache.nifi.processors.standard.MergeRecord")
            .position(PositionUtil.belowOf(transformToCsvHourlyFormat))
                .addConfigProperty("record-reader", csvReaderHourlyData.getId())
                .addConfigProperty("record-writer", csvWriterWeatherData.getId())
                .addConfigProperty("merge-strategy", "Defragment")
                .addConfigProperty("max-bin-age", "1 day")
            .autoTerminateAt("failure")
            .autoTerminateAt("original")
            .build();
    }

    public ProcessorEntity createAssignFilename() {
        return new ProcessorBuilder()
            .name("Assign Filename")
            .type("org.apache.nifi.processors.attributes.UpdateAttribute")
            .position(PositionUtil.lowerRightOf(mergeRecordsDailyFormat))
                .addConfigProperty("filename", "${weatherDataType}_${unit}_${time_retrieved:replace(' ' , '-')}_${city:replace(' ' , '-')}.csv")
            .build();
    }

    public ProcessorEntity createPutSftp() {
        return new ProcessorBuilder()
            .name("PutSftp(upload/historical)")
            .type("org.apache.nifi.processors.standard.PutSFTP")
            .position(PositionUtil.belowOf(assignFilename))
                .addConfigProperty("Hostname", "localhost")
                .addConfigProperty("Port", "2223")
                .addConfigProperty("Username", "mnunez")
                .addConfigProperty("Password", "ex1stgl0bal")
                .addConfigProperty("Remote Path", "upload/${weatherDataType}/${unit}")
            .autoTerminateAt("failure")
            .autoTerminateAt("reject")
            .autoTerminateAt("success")
            .build();
    }

}