package com.exist.nifirestapi.nifiremotesetup.port8080setup;

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

public class PersistIntoDatabasePGSetup {

    private NifiService nifiService;
    private ProcessGroupEntity processGroup;
    private PortEntity inputPort;
    private ProcessorEntity routeOnAttribute;
    private ProcessorEntity putSqlForecastDaily;
    private ProcessorEntity putSqlForecastHourly;
    private ProcessorEntity putSqlHistorical;

    private ControllerServiceEntity mySQLDBCP;

    public PersistIntoDatabasePGSetup() {}

    public PersistIntoDatabasePGSetup(NifiService nifiService, 
                                      ProcessGroupEntity processGroup,
                                      Map<String, ControllerServiceEntity> controllerServices) {

        this.nifiService = nifiService;
        this.processGroup = processGroup;
    }

    public void setup() {
        createControllerServices();
        enableControllerServices();
        addProcessors();
        connectProcessors();
    }

    public void createControllerServices() {

        mySQLDBCP = nifiService.addControllerService(
			new ControllerServiceBuilder()
				.name("mySQLDBCP")
				.type("org.apache.nifi.dbcp.DBCPConnectionPool")
					.addProperty("Database Connection URL", "jdbc:mysql://localhost:3306/weatherdatabase")
					.addProperty("Database Driver Class Name", "com.mysql.jdbc.Driver")
					.addProperty("database-driver-locations", "/home/mnunez/DBDriver/mysql-connector-java-8.0.11.jar")
					.addProperty("Database User", "root")
					.addProperty("Password", "ex1stgl0bal")
					.addProperty("Max Wait Time", "2000 millis")
					.addProperty("Max Total Connections", "20")
				.state("DISABLED")
				.build(),

            processGroup.getId()
        );
        
    }

    public void enableControllerServices() {
        nifiService.enableControllerService(mySQLDBCP);
    }

    public void addProcessors() {
        String processGroupId = this.processGroup.getId();

        inputPort            = nifiService.addInputPort(createInputPort(), processGroupId);

        routeOnAttribute     = nifiService.addProcessor(createRouteOnAttribute(), processGroupId);
        putSqlForecastDaily  = nifiService.addProcessor(createPutSqlForecastDaily(), processGroupId);
        putSqlForecastHourly = nifiService.addProcessor(createPutSqlForecastHourly(), processGroupId);
        putSqlHistorical     = nifiService.addProcessor(createPutSqlHistorical(), processGroupId);
    }

    public void connectProcessors() {
        String processGroupId = this.processGroup.getId();

        nifiService.connectComponents(inputPort, routeOnAttribute, Arrays.asList(), processGroupId);
        nifiService.connectComponents(routeOnAttribute, putSqlForecastDaily, Arrays.asList("forecast_daily"), processGroupId);
        nifiService.connectComponents(routeOnAttribute, putSqlForecastHourly, Arrays.asList("forecast_hourly"), processGroupId);
        nifiService.connectComponents(routeOnAttribute, putSqlHistorical, Arrays.asList("historical"), processGroupId);
    }

    public PortEntity getInputPort() {
        return inputPort;
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////

    public PortEntity createInputPort() {
        return new PortBuilder()
            .name("INCOMING CONVERTED DATA")
            .type("INPUT_PORT")
            .position(PositionUtil.startingPosition())
            .build();
    }

    public ProcessorEntity createRouteOnAttribute() {
        return new ProcessorBuilder()
            .name("Route Based on data type")
            .type("org.apache.nifi.processors.standard.RouteOnAttribute")
            .position(PositionUtil.belowOf(inputPort))
                .addConfigProperty("forecast_hourly", "${weatherDataType:equals('forecast_hourly')}")
                .addConfigProperty("forecast_daily", "${weatherDataType:equals('forecast_daily')}")
                .addConfigProperty("historical", "${weatherDataType:equals('historical')}")
            .autoTerminateAt("unmatched")
            .build();
    }

    public ProcessorEntity createPutSqlForecastDaily() {
        return new ProcessorBuilder()
            .name("Insert data into database (forecast_daily)")
            .type("org.apache.nifi.processors.standard.PutSQL")
            .position(PositionUtil.belowOf(routeOnAttribute))
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
            .build();
    }

    public ProcessorEntity createPutSqlForecastHourly() {
        return new ProcessorBuilder()
            .name("Insert data into database (forecast_hourly)")
            .type("org.apache.nifi.processors.standard.PutSQL")
            .position(PositionUtil.rightOf(putSqlForecastDaily))
                .addConfigProperty("JDBC Connection Pool", mySQLDBCP.getId())
                .addConfigProperty("putsql-sql-statement",
                    "INSERT INTO forecast_hourly_${unit} VALUES ("
                    + "'${city}',"
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

    public ProcessorEntity createPutSqlHistorical() {
        return new ProcessorBuilder()
            .name("Insert data into database (historical)")
            .type("org.apache.nifi.processors.standard.PutSQL")
            .position(PositionUtil.leftOf(putSqlForecastDaily))
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



}