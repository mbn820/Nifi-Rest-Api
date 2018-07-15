package com.exist.nifirestapi.nifisetup;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import com.exist.nifirestapi.builder.ControllerServiceBuilder;
import com.exist.nifirestapi.builder.ProcessGroupBuilder;
import com.exist.nifirestapi.service.NifiService;
import com.exist.nifirestapi.util.PositionUtil;

import org.apache.nifi.web.api.dto.PositionDTO;
import org.apache.nifi.web.api.entity.ControllerServiceEntity;
import org.apache.nifi.web.api.entity.ProcessGroupEntity;

public class NifiSetup {

	private NifiService nifiService;

	private ProcessGroupEntity parentProcessGroup;
	private ProcessGroupEntity locationKeysProcessGroup;
	private ProcessGroupEntity historicalDataProcessGroup;
	private ProcessGroupEntity forecastDailyDataProcessGroup;
	private ProcessGroupEntity forecastHourlyDataProcessGroup;

	private Map<String, ControllerServiceEntity> controllerServices = new HashMap<>();

	public NifiSetup(NifiService nifiService) {
		this.nifiService = nifiService;
	}

    public void start() {
		createProcessGroups();
		createControllerServices();
		// enableControllerServices();

		LocationKeysProcessGroupSetup locationKeyProcessGroupSetup =
			new LocationKeysProcessGroupSetup(nifiService, locationKeysProcessGroup, controllerServices);

		HistoricalDataProcessGroupSetup historicalDataProcessGroupSetup =
			new HistoricalDataProcessGroupSetup(nifiService, historicalDataProcessGroup, controllerServices);

		ForecastDataProcessGroupSetup forecastDailyProcessGroupSetup =
			new ForecastDataProcessGroupSetup(nifiService, forecastDailyDataProcessGroup, controllerServices);

		ForecastHourlyProcessGroupSetup forecastHourlyProcessGroupSetup =
			new ForecastHourlyProcessGroupSetup(nifiService, forecastHourlyDataProcessGroup, controllerServices);

		locationKeyProcessGroupSetup.setup();
		historicalDataProcessGroupSetup.setup();
		forecastDailyProcessGroupSetup.setup();
		forecastHourlyProcessGroupSetup.setup();

		nifiService.connectComponents(
			locationKeyProcessGroupSetup.getOutputPort(),
			historicalDataProcessGroupSetup.getInputPort(),
			Arrays.asList(),
			parentProcessGroup.getId());

		nifiService.connectComponents(
			locationKeyProcessGroupSetup.getOutputPort(),
			forecastDailyProcessGroupSetup.getInputPort(),
			Arrays.asList(),
			parentProcessGroup.getId());

		nifiService.connectComponents(
			locationKeyProcessGroupSetup.getOutputPort(),
			forecastHourlyProcessGroupSetup.getInputPort(),
			Arrays.asList(),
			parentProcessGroup.getId());
	}

	public void createProcessGroups() {

		this.parentProcessGroup = nifiService.addProcessGroup(
			new ProcessGroupBuilder()
				.name("Accuweather")
				.position(new PositionDTO(900.00, 900.00))
				.build(),

			"root"
		);

		this.locationKeysProcessGroup = nifiService.addProcessGroup(
            new ProcessGroupBuilder()
                .name("GET LOCATION KEYS")
                .position(new PositionDTO(900.00, 900.00))
				.build(),

			parentProcessGroup.getId()
        );

        this.historicalDataProcessGroup = nifiService.addProcessGroup(
            new ProcessGroupBuilder()
                .name("GET HISTORICAL DATA")
                .position(PositionUtil.rightOf(locationKeysProcessGroup))
                .build(),

			parentProcessGroup.getId()
        );

        this.forecastDailyDataProcessGroup = nifiService.addProcessGroup(
            new ProcessGroupBuilder()
                .name("GET DAILY FORECAST DATA")
                .position(PositionUtil.leftOf(locationKeysProcessGroup))
                .build(),

			parentProcessGroup.getId()
		);

		this.forecastHourlyDataProcessGroup = nifiService.addProcessGroup(
            new ProcessGroupBuilder()
                .name("GET HOURLY FORECAST DATA")
                .position(PositionUtil.belowOf(locationKeysProcessGroup))
                .build(),

			parentProcessGroup.getId()
        );
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

		ControllerServiceEntity mySQLDBCP = nifiService.addControllerService(
			new ControllerServiceBuilder()
				.name("mySQLDBCP")
				.type("org.apache.nifi.dbcp.DBCPConnectionPool")
					.addProperty("Database Connection URL", "jdbc:mysql://localhost:3306/weatherdatabase")
					.addProperty("Database Driver Class Name", "com.mysql.jdbc.Driver")
					.addProperty("database-driver-locations", "/home/mnunez/DBDriver/mysql-connector-java-8.0.11.jar")
					.addProperty("Database User", "root")
					.addProperty("Password", "")
					.addProperty("Max Wait Time", "2000 millis")
					.addProperty("Max Total Connections", "20")
				.state("DISABLED")
				.build(),

			parentProcessGroup.getId()
		);

		ControllerServiceEntity csvReaderHourlyData = nifiService.addControllerService(
			new ControllerServiceBuilder()
				.name("hourlyDataReader")
				.type("org.apache.nifi.csv.CSVReader")
					.addProperty("schema-access-strategy", "schema-text-property")
					.addProperty("schema-text", hourlyDataSchema)
				.state("DISABLED")
				.build(),

			parentProcessGroup.getId()
		);

		ControllerServiceEntity csvReaderDailyData = nifiService.addControllerService(
			new ControllerServiceBuilder()
				.name("dailyDataReader")
				.type("org.apache.nifi.csv.CSVReader")
					.addProperty("schema-access-strategy", "schema-text-property")
					.addProperty("schema-text", dailyDataSchema)
				.state("DISABLED")
				.build(),

			parentProcessGroup.getId()
		);

		ControllerServiceEntity csvWriterWeatherData = nifiService.addControllerService(
			new ControllerServiceBuilder()
				.name("weatherDataWriter")
				.type("org.apache.nifi.csv.CSVRecordSetWriter")
					.addProperty("Schema Write Strategy", "no-schema")
				.state("DISABLED")
				.build(),

			parentProcessGroup.getId()
		);


		controllerServices.put("mySQLDBCP", mySQLDBCP);
		controllerServices.put("csvReaderHourlyData", csvReaderHourlyData);
		controllerServices.put("csvReaderDailyData", csvReaderDailyData);
		controllerServices.put("csvWriterWeatherData", csvWriterWeatherData);
	}

	public void enableControllerServices() {
		controllerServices.forEach((name, controller) -> {
			controller.getComponent().setState("ENABLED");
			controller.getComponent().setProperties(null);
			nifiService.updateControllerService(controller);
		});
	}

}

// branch remoteProcessGroup
