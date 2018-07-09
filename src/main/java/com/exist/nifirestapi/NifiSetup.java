package com.exist.nifirestapi;

import com.exist.nifirestapi.builder.ControllerServiceBuilder;
import com.exist.nifirestapi.builder.ProcessGroupBuilder;
import com.exist.nifirestapi.builder.ProcessorBuilder;
import com.exist.nifirestapi.service.NifiService;
import com.exist.nifirestapi.util.PositionUtil;

import org.apache.nifi.web.api.dto.PositionDTO;
import org.apache.nifi.web.api.entity.ControllerServiceEntity;
import org.apache.nifi.web.api.entity.ProcessGroupEntity;
import org.apache.nifi.web.api.entity.ProcessorEntity;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

@Component
public class NifiSetup {

    @Autowired
    private NifiService nifiService;
    
    private static final String HISTORICAL_RECORD_READER = "01641025-0dc2-19e8-133a-26025900b76e";
    private static final String HISTORICAL_RECORD_WRITER = "01641039-0dc2-19e8-d392-ea0e77d1c55c";


    @Bean
    public CommandLineRunner run() {
        return args -> {
            setUp();
        };
    }

    public void setUp() {

        // *************************** CREATE PROCESS GROUPS *************************** //

        // ProcessGroupEntity locationKeysProcessGroup = nifiService.addProcessGroup(
        //     new ProcessGroupBuilder()
        //         .name("GET LOCATION KEYS")
        //         .position(new PositionDTO(900.00, 900.00))
        //         .build()
        // );

        // ProcessGroupEntity historicalDataProcessGroup = nifiService.addProcessGroup(
        //     new ProcessGroupBuilder()
        //         .name("GET HISTORICAL DATA")
        //         .position(PositionUtil.rightOf(locationKeysProcessGroup))
        //         .build()
        // );

        // ProcessGroupEntity forecastDataProcessGroup = nifiService.addProcessGroup(
        //     new ProcessGroupBuilder()
        //         .name("GET FORECAST DATA")
        //         .position(PositionUtil.leftOf(locationKeysProcessGroup))
        //         .build()
        // );

        // *************************** CREATE CONTROLLER SERVICE *************************** //

        ControllerServiceEntity mySqlDbcp = nifiService.addControllerService(
			new ControllerServiceBuilder()
	            .name("MYSQLDBCP")
	            .type("org.apache.nifi.dbcp.DBCPConnectionPool")
				.state("DISABLED")
	                .addProperty("Database Connection URL", "jdbc:mysql://localhost:3306/weatherdatabase")
					.addProperty("Database Driver Class Name", "com.mysql.jdbc.Driver")
					.addProperty("database-driver-locations", "/home/mnunez/DBDriver/mysql-connector-java-8.0.11.jar")
					.addProperty("Database User", "root")
					.addProperty("Password", "ex1stgl0bal")
				.build()
		);

		// *************************** GET LOCATION KEYS *************************** //

		ProcessorEntity loadCities = nifiService.addProcessor(
			new ProcessorBuilder()
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
				.build()
		);

		ProcessorEntity assignTimeStamp = nifiService.addProcessor(
			new ProcessorBuilder()
				.name("Assign Timestamp")
				.type("org.apache.nifi.processors.attributes.UpdateAttribute")
				.position(PositionUtil.belowOf(loadCities))
					.addConfigProperty("apiKey", "qQyAa99QwlKLMop22S9KgDgu5px1lHcT")
					.addConfigProperty("time_retrieved", "${now():format('yyyy-MM-dd HH:mm:ss')}")
				.build()
		);

		ProcessorEntity splitCities = nifiService.addProcessor(
			new ProcessorBuilder()
				.name("Split into individual cities")
				.type("org.apache.nifi.processors.standard.SplitJson")
				.position(PositionUtil.belowOf(assignTimeStamp))
					.addConfigProperty("JsonPath Expression", "$.*")
				.autoTerminateAt("failure")
				.autoTerminateAt("original")
				.build()
		);

		ProcessorEntity extractCity = nifiService.addProcessor(
			new ProcessorBuilder()
				.name("Extract city name to attribute")
				.type("org.apache.nifi.processors.standard.EvaluateJsonPath")
				.position(PositionUtil.belowOf(splitCities))
					.addConfigProperty("Destination", "flowfile-attribute")
					.addConfigProperty("city", "$.city")
				.autoTerminateAt("failure")
				.autoTerminateAt("unmatched")
				.build()
		);

		ProcessorEntity getLocationKeys = nifiService.addProcessor(
			new ProcessorBuilder()
				.name("Get location keys using accuweather locations api")
				.type("org.apache.nifi.processors.standard.InvokeHTTP")
				.position(PositionUtil.belowOf(extractCity))
					.addConfigProperty("Remote URL", "https://dataservice.accuweather.com/locations/v1/cities/ph/search?apikey=${apikey}&q=${city}")
					.addConfigProperty("Proxy Type", "http")
				.autoTerminateAt("Failure")
				.autoTerminateAt("No Retry")
				.autoTerminateAt("Original")
				.autoTerminateAt("Retry")
				.build()
		);

		ProcessorEntity extractLocationKey = nifiService.addProcessor(
			new ProcessorBuilder()
				.name("Extract location key to attribute")
				.type("org.apache.nifi.processors.standard.EvaluateJsonPath")
				.position(PositionUtil.belowOf(getLocationKeys))
					.addConfigProperty("Destination", "flowfile-attribute")
					.addConfigProperty("city", "$[0].EnglishName")
					.addConfigProperty("locationKey", "$[0].Key")
				.autoTerminateAt("failure")
				.autoTerminateAt("matched")
				.autoTerminateAt("unmatched")
				.build()
		);

		// *************************** DB HISTORICAL *************************** //

		ProcessorEntity getHistoricalData = nifiService.addProcessor(
			new ProcessorBuilder()
				.name("Get historical data from accuweather api")
				.type("org.apache.nifi.processors.standard.InvokeHTTP")
				.position(PositionUtil.rightOf(extractLocationKey))
					.addConfigProperty("Remote URL", "https://dataservice.accuweather.com/currentconditions/v1/${locationKey}/historical/24?apikey=${apikey}&details=true")
					.addConfigProperty("Proxy Type", "http")
				.autoTerminateAt("Failure")
				.autoTerminateAt("No Retry")
				.autoTerminateAt("Original")
				.autoTerminateAt("Retry")
				.build()
		);

		ProcessorEntity splitHistoricalData = nifiService.addProcessor(
			new ProcessorBuilder()
				.name("Split into individual hourly data")
				.type("org.apache.nifi.processors.standard.SplitJson")
				.position(PositionUtil.belowOf(getHistoricalData))
					.addConfigProperty("JsonPath Expression", "$.*")
				.autoTerminateAt("failure")
				.autoTerminateAt("original")
				.build()
		);

		ProcessorEntity extractFields = nifiService.addProcessor(
			new ProcessorBuilder()
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
				.build()
        );
        
        ProcessorEntity convertTemperature = nifiService.addProcessor(
			new ProcessorBuilder()
				.name("PutSftp(upload/historical)")
				.type("exist.processors.sample.TemperatureConvert")
				.position(PositionUtil.belowOf(extractFields))
				.build()
		);

		ProcessorEntity persistIntoDatabase = nifiService.addProcessor(
			new ProcessorBuilder()
				.name("Insert data into database")
				.type("org.apache.nifi.processors.standard.PutSQL")
				.position(PositionUtil.belowOf(convertTemperature))
					.addConfigProperty("JDBC Connection Pool", mySqlDbcp.getId())
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
				.build()
        );
        
        // *************************** CSV HISTORICAL *************************** //

        ProcessorEntity transformToCsv = nifiService.addProcessor(
			new ProcessorBuilder()
				.name("Transform into csv format")
				.type("org.apache.nifi.processors.standard.ReplaceText")
				.position(PositionUtil.rightOf(convertTemperature))
					.addConfigProperty("Replacement Value", "${city},${locationKey},${date_time},${temp},${humidity},${sky_condition},${time_retrieved}")
				.autoTerminateAt("failure")
				.build()
        );
        
        ProcessorEntity mergeRecords = nifiService.addProcessor(
			new ProcessorBuilder()
				.name("Merge Records")
				.type("org.apache.nifi.processors.standard.MergeRecord")
				.position(PositionUtil.belowOf(transformToCsv))
                    .addConfigProperty("record-reader", HISTORICAL_RECORD_READER)
                    .addConfigProperty("record-writer", HISTORICAL_RECORD_WRITER)
                    .addConfigProperty("merge-strategy", "Defragment")
                .autoTerminateAt("failure")
                .autoTerminateAt("original")
				.build()
        );
        
        ProcessorEntity assignFilename = nifiService.addProcessor(
			new ProcessorBuilder()
				.name("Assign Filename")
				.type("org.apache.nifi.processors.attributes.UpdateAttribute")
				.position(PositionUtil.belowOf(mergeRecords))
					.addConfigProperty("filename", "historical_${unit}_${time_retrieved:replace(' ' , '-')}_${city:replace(' ' , '-')}.csv")
				.build()
        );
        
        ProcessorEntity putSftpHistorical = nifiService.addProcessor(
			new ProcessorBuilder()
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
				.build()
		);

		// *************************** DB FORECAST_DAILY *************************** //

		
		



		// *************************** CREATE CONNECTIONS *************************** //

        // location keys
		nifiService.connectProcessors(loadCities, assignTimeStamp, "success");
		nifiService.connectProcessors(assignTimeStamp, splitCities, "success");
		nifiService.connectProcessors(splitCities, extractCity, "split");
		nifiService.connectProcessors(extractCity, getLocationKeys, "matched");
		nifiService.connectProcessors(getLocationKeys, extractLocationKey, "Response");

        // to db(historical)
		nifiService.connectProcessors(extractLocationKey, getHistoricalData, "matched");
		nifiService.connectProcessors(getHistoricalData, splitHistoricalData, "Response");
		nifiService.connectProcessors(splitHistoricalData, extractFields, "split");
        nifiService.connectProcessors(extractFields, convertTemperature, "matched");
        nifiService.connectProcessors(convertTemperature, persistIntoDatabase, "CELSIUS", "FAHRENHEIT", "KELVIN");
        // to csv(historical)
        nifiService.connectProcessors(convertTemperature, transformToCsv, "CELSIUS", "FAHRENHEIT", "KELVIN");
        nifiService.connectProcessors(transformToCsv, mergeRecords, "success");
        nifiService.connectProcessors(mergeRecords, assignFilename, "merged");
        nifiService.connectProcessors(assignFilename, putSftpHistorical, "success");
    }

}