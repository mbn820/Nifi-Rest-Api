package com.exist.nifirestapi;

import java.util.ArrayList;
import java.util.Arrays;

import com.exist.nifirestapi.builder.ControllerServiceBuilder;
import com.exist.nifirestapi.builder.PortBuilder;
import com.exist.nifirestapi.builder.ProcessGroupBuilder;
import com.exist.nifirestapi.builder.ProcessorBuilder;
import com.exist.nifirestapi.service.NifiService;
import com.exist.nifirestapi.util.PositionUtil;

import org.apache.nifi.web.api.dto.PositionDTO;
import org.apache.nifi.web.api.entity.ControllerServiceEntity;
import org.apache.nifi.web.api.entity.PortEntity;
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

	private ControllerServiceEntity mySQLDBCP;
	
	private ProcessGroupEntity locationKeysProcessGroup;
	private ProcessGroupEntity historicalDataProcessGroup;
	private ProcessGroupEntity forecastDailyDataProcessGroup;

	private PortEntity locationKeysInputPortHistorical;
	private PortEntity locationKeysInputPortForecastDaily;
	private PortEntity locationKeysOutputPort;
    
    private static final String HISTORICAL_RECORD_READER = "01641025-0dc2-19e8-133a-26025900b76e";
	private static final String HISTORICAL_RECORD_WRITER = "01641026-0dc2-19e8-513d-e21dd383dfc1";
	private static final String FORECAST_DAILY_RECORD_READER = "01641038-0dc2-19e8-df00-e8d06e76515f";
    private static final String FORECAST_DAILY_RECORD_WRITER = "01641039-0dc2-19e8-d392-ea0e77d1c55c";


    @Bean
    public CommandLineRunner run() {
        return args -> {
			createMysqlControllerService();
			createProcessGroups();
			locationKeysProcessGroupSetup();
			historicalDataProcessGroupSetup();
			forecastDailyDataProcessGroupSetup();
			connectProcessGroups();
        };
	}

	public void createMysqlControllerService() {
		this.mySQLDBCP = nifiService.addControllerService(
			new ControllerServiceBuilder()
	            .name("mySQLDBCP")
	            .type("org.apache.nifi.dbcp.DBCPConnectionPool")
				.state("DISABLED")
	                .addProperty("Database Connection URL", "jdbc:mysql://localhost:3306/weatherdatabase")
					.addProperty("Database Driver Class Name", "com.mysql.jdbc.Driver")
					.addProperty("database-driver-locations", "/home/mnunez/DBDriver/mysql-connector-java-8.0.11.jar")
					.addProperty("Database User", "root")
					.addProperty("Password", "ex1stgl0bal")
				.build(),

			"root"
		);
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

	// ********************************************************************************** //

	public void locationKeysProcessGroupSetup() {
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
				.build(),

			this.locationKeysProcessGroup.getId()
		);

		ProcessorEntity assignTimeStamp = nifiService.addProcessor(
			new ProcessorBuilder()
				.name("Assign Timestamp")
				.type("org.apache.nifi.processors.attributes.UpdateAttribute")
				.position(PositionUtil.belowOf(loadCities))
					.addConfigProperty("apiKey", "qQyAa99QwlKLMop22S9KgDgu5px1lHcT")
					.addConfigProperty("time_retrieved", "${now():format('yyyy-MM-dd HH:mm:ss')}")
				.build(),

			this.locationKeysProcessGroup.getId()
		);

		ProcessorEntity splitCities = nifiService.addProcessor(
			new ProcessorBuilder()
				.name("Split into individual cities")
				.type("org.apache.nifi.processors.standard.SplitJson")
				.position(PositionUtil.belowOf(assignTimeStamp))
					.addConfigProperty("JsonPath Expression", "$.*")
				.autoTerminateAt("failure")
				.autoTerminateAt("original")
				.build(),

			this.locationKeysProcessGroup.getId()
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
				.build(),

			this.locationKeysProcessGroup.getId()
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
				.build(),

			this.locationKeysProcessGroup.getId()
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
				.build(),

			this.locationKeysProcessGroup.getId()
		);

		this.locationKeysOutputPort = nifiService.addOutputPort(
			new PortBuilder()
				.name("LOCATION KEYS OUTPUT PORT")
				.type("OUTPUT PORT")
				.position(PositionUtil.belowOf(extractLocationKey))
				.build(),

			this.locationKeysProcessGroup.getId()
		);

		nifiService.connectProcessors(loadCities, assignTimeStamp, Arrays.asList("success"), this.locationKeysProcessGroup.getId());
		nifiService.connectProcessors(assignTimeStamp, splitCities, Arrays.asList("success"), this.locationKeysProcessGroup.getId());
		nifiService.connectProcessors(splitCities, extractCity, Arrays.asList("split"), this.locationKeysProcessGroup.getId());
		nifiService.connectProcessors(extractCity, getLocationKeys, Arrays.asList("matched"), this.locationKeysProcessGroup.getId());
		nifiService.connectProcessors(getLocationKeys, extractLocationKey, Arrays.asList("Response"), this.locationKeysProcessGroup.getId());

		nifiService.connectProcessorToPort(extractLocationKey, locationKeysOutputPort, Arrays.asList("matched"), this.locationKeysProcessGroup.getId());
	}

	// ********************************************************************************** //

	public void historicalDataProcessGroupSetup() {

		this.locationKeysInputPortHistorical = nifiService.addInputPort(
			new PortBuilder()
				.name("LOCATION KEYS INPUT PORT")
				.type("INPUT_PORT")
				.position(new PositionDTO(1000.00, 1000.00))
				.build(),

			this.historicalDataProcessGroup.getId()
		);

		ProcessorEntity getHistoricalData = nifiService.addProcessor(
			new ProcessorBuilder()
				.name("Get historical data from accuweather api")
				.type("org.apache.nifi.processors.standard.InvokeHTTP")
				.position(PositionUtil.belowOf(locationKeysInputPortHistorical))
					.addConfigProperty("Remote URL", "https://dataservice.accuweather.com/currentconditions/v1/${locationKey}/historical/24?apikey=${apikey}&details=true")
					.addConfigProperty("Proxy Type", "http")
				.autoTerminateAt("Failure")
				.autoTerminateAt("No Retry")
				.autoTerminateAt("Original")
				.autoTerminateAt("Retry")
				.build(),

			this.historicalDataProcessGroup.getId()
		);

		ProcessorEntity splitHistoricalData = nifiService.addProcessor(
			new ProcessorBuilder()
				.name("Split into individual hourly data")
				.type("org.apache.nifi.processors.standard.SplitJson")
				.position(PositionUtil.belowOf(getHistoricalData))
					.addConfigProperty("JsonPath Expression", "$.*")
				.autoTerminateAt("failure")
				.autoTerminateAt("original")
				.build(),

			this.historicalDataProcessGroup.getId()
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
				.build(),

			this.historicalDataProcessGroup.getId()
        );
        
        ProcessorEntity convertTemperature = nifiService.addProcessor(
			new ProcessorBuilder()
				.name("PutSftp(upload/historical)")
				.type("exist.processors.sample.TemperatureConvert")
				.position(PositionUtil.belowOf(extractFields))
				.build(),

			this.historicalDataProcessGroup.getId()
		);

		ProcessorEntity persistIntoDatabase = nifiService.addProcessor(
			new ProcessorBuilder()
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
				.build(),

			this.historicalDataProcessGroup.getId()
        );
        
        // *************************** CSV HISTORICAL *************************** //

        ProcessorEntity transformToCsv = nifiService.addProcessor(
			new ProcessorBuilder()
				.name("Transform into csv format")
				.type("org.apache.nifi.processors.standard.ReplaceText")
				.position(PositionUtil.rightOf(convertTemperature))
					.addConfigProperty("Replacement Value", "${city},${locationKey},${date_time},${temp},${humidity},${sky_condition},${time_retrieved}")
				.autoTerminateAt("failure")
				.build(),

			this.historicalDataProcessGroup.getId()
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
				.build(),

			this.historicalDataProcessGroup.getId()
        );
        
        ProcessorEntity assignFilename = nifiService.addProcessor(
			new ProcessorBuilder()
				.name("Assign Filename")
				.type("org.apache.nifi.processors.attributes.UpdateAttribute")
				.position(PositionUtil.belowOf(mergeRecords))
					.addConfigProperty("filename", "historical_${unit}_${time_retrieved:replace(' ' , '-')}_${city:replace(' ' , '-')}.csv")
				.build(),

			this.historicalDataProcessGroup.getId()
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
				.build(),

			this.historicalDataProcessGroup.getId()
		);


		nifiService.connectPortToProcessor(locationKeysInputPortHistorical, getHistoricalData, new ArrayList<String>(), this.historicalDataProcessGroup.getId());
		// to db(historical)
		nifiService.connectProcessors(getHistoricalData, splitHistoricalData, Arrays.asList("Response"), this.historicalDataProcessGroup.getId());
		nifiService.connectProcessors(splitHistoricalData, extractFields, Arrays.asList("split"), this.historicalDataProcessGroup.getId());
        nifiService.connectProcessors(extractFields, convertTemperature, Arrays.asList("matched"), this.historicalDataProcessGroup.getId());
        nifiService.connectProcessors(convertTemperature, persistIntoDatabase, Arrays.asList("CELSIUS", "FAHRENHEIT", "KELVIN"), this.historicalDataProcessGroup.getId());
        // to csv(historical)
        nifiService.connectProcessors(convertTemperature, transformToCsv, Arrays.asList("CELSIUS", "FAHRENHEIT", "KELVIN"), this.historicalDataProcessGroup.getId());
        nifiService.connectProcessors(transformToCsv, mergeRecords, Arrays.asList("success"), this.historicalDataProcessGroup.getId());
        nifiService.connectProcessors(mergeRecords, assignFilename, Arrays.asList("merged"), this.historicalDataProcessGroup.getId());
        nifiService.connectProcessors(assignFilename, putSftpHistorical, Arrays.asList("success"), this.historicalDataProcessGroup.getId());
	}

	// ********************************************************************************** //

	public void forecastDailyDataProcessGroupSetup() {

		this.locationKeysInputPortForecastDaily = nifiService.addInputPort(
			new PortBuilder()
				.name("LOCATION KEY INPUT PORT FORECAST DAILY")
				.type("INPUT_PORT")
				.position(new PositionDTO(1000.00, 1000.00))
				.build(),

			this.forecastDailyDataProcessGroup.getId()
		);

		ProcessorEntity getForecastDailyData = nifiService.addProcessor(
			new ProcessorBuilder()
				.name("Get forecast data from accuweather api")
				.type("org.apache.nifi.processors.standard.InvokeHTTP")
				.position(PositionUtil.belowOf(locationKeysInputPortForecastDaily))
					.addConfigProperty("Remote URL", "https://dataservice.accuweather.com/forecasts/v1/daily/5day/${locationKey}?apikey=${apikey}&metric=true")
					.addConfigProperty("Proxy Type", "http")
				.autoTerminateAt("Failure")
				.autoTerminateAt("No Retry")
				.autoTerminateAt("Original")
				.autoTerminateAt("Retry")
				.build(),

			this.forecastDailyDataProcessGroup.getId()
		);

		ProcessorEntity splitForecastDailyData = nifiService.addProcessor(
			new ProcessorBuilder()
				.name("Split into individual daily data")
				.type("org.apache.nifi.processors.standard.SplitJson")
				.position(PositionUtil.belowOf(getForecastDailyData))
					.addConfigProperty("JsonPath Expression", "$.DailyForecasts")
				.autoTerminateAt("failure")
				.autoTerminateAt("original")
				.build(),

			this.forecastDailyDataProcessGroup.getId()
		);

		ProcessorEntity extractFields = nifiService.addProcessor(
			new ProcessorBuilder()
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
				.build(),

			this.forecastDailyDataProcessGroup.getId()
        );
        
        ProcessorEntity convertTemperature = nifiService.addProcessor(
			new ProcessorBuilder()
				.name("PutSftp(upload/historical)")
				.type("exist.processors.sample.TemperatureConvert")
				.position(PositionUtil.belowOf(extractFields))
				.build(),

			this.forecastDailyDataProcessGroup.getId()
		);

		ProcessorEntity persistIntoDatabase = nifiService.addProcessor(
			new ProcessorBuilder()
				.name("Insert data into database")
				.type("org.apache.nifi.processors.standard.PutSQL")
				.position(PositionUtil.belowOf(convertTemperature))
					.addConfigProperty("JDBC Connection Pool", mySQLDBCP.getId())
					.addConfigProperty("putsql-sql-statement",
						"INSERT INTO forecast_daily_${unit} VALUES ("
						  +	"'${city}',"
						  + "'${locationKey}',"
						  + "'${date:replace(\"T\", \" \"):substringBefore(\"+\")}',"
						  + "'${temp_min}',"
						  + "'${temp_max}',"
                          + "'${unit}',"
						  + "'${day_condition}',"
						  + "'${night_condition}',"
						  + "'${time_retrieved}')"
					)
					.addConfigProperty("Support Fragmented Transactions", "false")
				.autoTerminateAt("failure")
				.autoTerminateAt("retry")
				.autoTerminateAt("success")
				.build(),

			this.forecastDailyDataProcessGroup.getId()
        );
        
        // *************************** CSV FORECAST_DAILY *************************** //

        ProcessorEntity transformToCsv = nifiService.addProcessor(
			new ProcessorBuilder()
				.name("Transform into csv format")
				.type("org.apache.nifi.processors.standard.ReplaceText")
				.position(PositionUtil.rightOf(convertTemperature))
					.addConfigProperty("Replacement Value", "${city},${locationKey},${date},${temp_min},${temp_max},${day_condition},${night_condition},${time_retrieved}")
				.autoTerminateAt("failure")
				.build(),

			this.forecastDailyDataProcessGroup.getId()
        );
        
        ProcessorEntity mergeRecords = nifiService.addProcessor(
			new ProcessorBuilder()
				.name("Merge Records")
				.type("org.apache.nifi.processors.standard.MergeRecord")
				.position(PositionUtil.belowOf(transformToCsv))
                    .addConfigProperty("record-reader", FORECAST_DAILY_RECORD_READER)
                    .addConfigProperty("record-writer", FORECAST_DAILY_RECORD_WRITER)
                    .addConfigProperty("merge-strategy", "Defragment")
                .autoTerminateAt("failure")
                .autoTerminateAt("original")
				.build(),

			this.forecastDailyDataProcessGroup.getId()
        );
        
        ProcessorEntity assignFilename = nifiService.addProcessor(
			new ProcessorBuilder()
				.name("Assign Filename")
				.type("org.apache.nifi.processors.attributes.UpdateAttribute")
				.position(PositionUtil.belowOf(mergeRecords))
					.addConfigProperty("filename", "forecast_daily_${unit}_${time_retrieved:replace(' ' , '-')}_${city:replace(' ' , '-')}.csv")
				.build(),

			this.forecastDailyDataProcessGroup.getId()
        );
        
        ProcessorEntity putSftpForecastDaily = nifiService.addProcessor(
			new ProcessorBuilder()
				.name("PutSftp(upload/forecast_daily)")
				.type("org.apache.nifi.processors.standard.PutSFTP")
				.position(PositionUtil.belowOf(assignFilename))
                    .addConfigProperty("Hostname", "localhost")
                    .addConfigProperty("Port", "2223")
                    .addConfigProperty("Username", "mnunez")
                    .addConfigProperty("Password", "ex1stgl0bal")
                    .addConfigProperty("Remote Path", "upload/forecast_daily/${unit}")
                .autoTerminateAt("failure")
                .autoTerminateAt("reject")
                .autoTerminateAt("success")
				.build(),

			this.forecastDailyDataProcessGroup.getId()
		);

		nifiService.connectPortToProcessor(this.locationKeysInputPortForecastDaily, getForecastDailyData, new ArrayList<String>(), this.forecastDailyDataProcessGroup.getId());
		// to db(forecast_daily)
		nifiService.connectProcessors(getForecastDailyData, splitForecastDailyData, Arrays.asList("Response"), this.forecastDailyDataProcessGroup.getId());
		nifiService.connectProcessors(splitForecastDailyData, extractFields, Arrays.asList("split"), this.forecastDailyDataProcessGroup.getId());
        nifiService.connectProcessors(extractFields, convertTemperature, Arrays.asList("matched"), this.forecastDailyDataProcessGroup.getId());
        nifiService.connectProcessors(convertTemperature, persistIntoDatabase, Arrays.asList("CELSIUS", "FAHRENHEIT", "KELVIN"), this.forecastDailyDataProcessGroup.getId());
        // to csv(forecast_daily)
        nifiService.connectProcessors(convertTemperature, transformToCsv, Arrays.asList("CELSIUS", "FAHRENHEIT", "KELVIN"), this.forecastDailyDataProcessGroup.getId());
        nifiService.connectProcessors(transformToCsv, mergeRecords, Arrays.asList("success"), this.forecastDailyDataProcessGroup.getId());
        nifiService.connectProcessors(mergeRecords, assignFilename, Arrays.asList("merged"), this.forecastDailyDataProcessGroup.getId());
        nifiService.connectProcessors(assignFilename, putSftpForecastDaily, Arrays.asList("success"), this.forecastDailyDataProcessGroup.getId());
	}

	public void connectProcessGroups() {
		nifiService.connectPorts(this.locationKeysOutputPort, this.locationKeysInputPortHistorical, new ArrayList<String>(), "root");
		nifiService.connectPorts(this.locationKeysOutputPort, this.locationKeysInputPortForecastDaily, new ArrayList<String>(), "root");
	}



}