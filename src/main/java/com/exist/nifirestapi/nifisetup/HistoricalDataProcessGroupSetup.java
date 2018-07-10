package com.exist.nifirestapi.nifisetup;

import java.util.ArrayList;
import java.util.Arrays;

import com.exist.nifirestapi.builder.ControllerServiceBuilder;
import com.exist.nifirestapi.builder.PortBuilder;
import com.exist.nifirestapi.builder.ProcessorBuilder;
import com.exist.nifirestapi.service.NifiService;
import com.exist.nifirestapi.util.PositionUtil;

import org.apache.nifi.web.api.dto.PositionDTO;
import org.apache.nifi.web.api.entity.ControllerServiceEntity;
import org.apache.nifi.web.api.entity.PortEntity;
import org.apache.nifi.web.api.entity.ProcessGroupEntity;
import org.apache.nifi.web.api.entity.ProcessorEntity;

public class HistoricalDataProcessGroupSetup {

    private NifiService nifiService;
    private ControllerServiceEntity mySQLDBCP;
    private ProcessGroupEntity locationKeysProcessGroup;
    private ProcessorEntity getHistoricalData;
    private ProcessorEntity splitHistoricalData;
    private ProcessorEntity extractFields;
    private ProcessorEntity convertTemperature;
    private ProcessorEntity persistIntoDatabase;
    private ProcessorEntity transformToCsv;
    private ProcessorEntity mergeRecords;
    private ProcessorEntity assignFilename;
    private ProcessorEntity putSftp;
    private PortEntity locationKeysInputPort;

    private static final String HISTORICAL_RECORD_READER = "01641025-0dc2-19e8-133a-26025900b76e";
    private static final String HISTORICAL_RECORD_WRITER = "01641026-0dc2-19e8-513d-e21dd383dfc1";

    public HistoricalDataProcessGroupSetup() {}
    
    public HistoricalDataProcessGroupSetup(NifiService nifiService, ProcessGroupEntity processGroup) {
        this.nifiService = nifiService;
        this.locationKeysProcessGroup = processGroup;
    }

    public void setup() {
        addProcessors();
        connectProcessors();
    }

    public void addProcessors() {
        String processGroupId = this.locationKeysProcessGroup.getId();

        mySQLDBCP = nifiService.addControllerService(createMysqlControllerService(), processGroupId);
        locationKeysInputPort = nifiService.addInputPort(createLocationKeyInputPort(), processGroupId);

        getHistoricalData   = nifiService.addProcessor(createGetHistoricalData(), processGroupId);
        splitHistoricalData = nifiService.addProcessor(createSplitHistoricalData(), processGroupId);
        extractFields       = nifiService.addProcessor(createExtractFields(), processGroupId);
        convertTemperature  = nifiService.addProcessor(createConvertTemperature(), processGroupId);
        persistIntoDatabase = nifiService.addProcessor(createPersistIntoDatabase(), processGroupId);
        transformToCsv      = nifiService.addProcessor(createTransformToCsv(), processGroupId);
        mergeRecords        = nifiService.addProcessor(createMergeRecords(), processGroupId);
        assignFilename      = nifiService.addProcessor(createAssignFilename(), processGroupId);
        putSftp             = nifiService.addProcessor(createPutSftp(), processGroupId);
    }

    public void connectProcessors() {
        String processGroupId = this.locationKeysProcessGroup.getId();

        nifiService.connectComponents(locationKeysInputPort, getHistoricalData, new ArrayList<String>(), processGroupId);
		// to db
		nifiService.connectComponents(getHistoricalData, splitHistoricalData, Arrays.asList("Response"), processGroupId);
		nifiService.connectComponents(splitHistoricalData, extractFields, Arrays.asList("split"), processGroupId);
        nifiService.connectComponents(extractFields, convertTemperature, Arrays.asList("matched"), processGroupId);
        nifiService.connectComponents(convertTemperature, persistIntoDatabase, Arrays.asList("CELSIUS", "FAHRENHEIT", "KELVIN"), processGroupId);
        // to csv
        nifiService.connectComponents(convertTemperature, transformToCsv, Arrays.asList("CELSIUS", "FAHRENHEIT", "KELVIN"), processGroupId);
        nifiService.connectComponents(transformToCsv, mergeRecords, Arrays.asList("success"), processGroupId);
        nifiService.connectComponents(mergeRecords, assignFilename, Arrays.asList("merged"), processGroupId);
        nifiService.connectComponents(assignFilename, putSftp, Arrays.asList("success"), processGroupId);
    }

    public PortEntity getInputPort() {
        return locationKeysInputPort;
    }

    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public ControllerServiceEntity createMysqlControllerService() {
		return new ControllerServiceBuilder()
	        .name("mySQLDBCP")
	        .type("org.apache.nifi.dbcp.DBCPConnectionPool")
	            .addProperty("Database Connection URL", "jdbc:mysql://localhost:3306/weatherdatabase")
                .addProperty("Database Driver Class Name", "com.mysql.jdbc.Driver")
                .addProperty("database-driver-locations", "/home/mnunez/DBDriver/mysql-connector-java-8.0.11.jar")
				.addProperty("Database User", "root")
				.addProperty("Password", "ex1stgl0bal")
			.state("DISABLED")
			.build();
	}

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

    public ProcessorEntity createConvertTemperature() {
        return new ProcessorBuilder()
            .name("PutSftp(upload/historical)")
            .type("exist.processors.sample.TemperatureConvert")
            .position(PositionUtil.belowOf(extractFields))
            .build();
    }

    public ProcessorEntity createPersistIntoDatabase() {
        return new ProcessorBuilder()
            .name("Insert data into database")
            .type("org.apache.nifi.processors.standard.PutSQL")
            .position(PositionUtil.belowOf(convertTemperature))
                .addConfigProperty("JDBC Connection Pool", mySQLDBCP.getId())
                .addConfigProperty("putsql-sql-statement",
                    "INSERT INTO historical_${unit} VALUES ("
                    +	"'${city}',"
                    + "'${locationKey}',"
                    + "'${date_time:replace(\"T\", \" \"):substringBefore(\"+\")}',"
                    + "'${temp}',"
                    + "'${unit}',"
                    + "'${humidity}',"
                    + "'${sky_condition}',"
                    + "'${time_retrieved}')"
                )
                .addConfigProperty("Support Fragmented Transactions", "false")
            .autoTerminateAt("failure")
            .autoTerminateAt("retry")
            .autoTerminateAt("success")
            .build();
    }

    public ProcessorEntity createTransformToCsv() {
        return new ProcessorBuilder()
            .name("Transform into csv format")
            .type("org.apache.nifi.processors.standard.ReplaceText")
            .position(PositionUtil.rightOf(convertTemperature))
                .addConfigProperty("Replacement Value", "${city},${locationKey},${date_time},${temp},${humidity},${sky_condition},${time_retrieved}")
            .autoTerminateAt("failure")
            .build();
    }

    public ProcessorEntity createMergeRecords() {
        return new ProcessorBuilder()
            .name("Merge Records")
            .type("org.apache.nifi.processors.standard.MergeRecord")
            .position(PositionUtil.belowOf(transformToCsv))
                .addConfigProperty("record-reader", HISTORICAL_RECORD_READER)
                .addConfigProperty("record-writer", HISTORICAL_RECORD_WRITER)
                .addConfigProperty("merge-strategy", "Defragment")
            .autoTerminateAt("failure")
            .autoTerminateAt("original")
            .build();
    }

    public ProcessorEntity createAssignFilename() {
        return new ProcessorBuilder()
            .name("Assign Filename")
            .type("org.apache.nifi.processors.attributes.UpdateAttribute")
            .position(PositionUtil.belowOf(mergeRecords))
                .addConfigProperty("filename", "historical_${unit}_${time_retrieved:replace(' ' , '-')}_${city:replace(' ' , '-')}.csv")
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
                .addConfigProperty("Remote Path", "upload/historical/${unit}")
            .autoTerminateAt("failure")
            .autoTerminateAt("reject")
            .autoTerminateAt("success")
            .build();
    }

}