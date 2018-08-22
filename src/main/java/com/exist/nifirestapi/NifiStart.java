package com.exist.nifirestapi;

import com.exist.nifirestapi.builder.ControllerServiceBuilder;
import com.exist.nifirestapi.builder.ProcessorBuilder;
import com.exist.nifirestapi.service.NifiService;
import com.exist.nifirestapi.util.PositionUtil;
import org.apache.nifi.web.api.dto.PositionDTO;
import org.apache.nifi.web.api.entity.ControllerServiceEntity;
import org.apache.nifi.web.api.entity.ControllerServiceReferencingComponentEntity;
import org.apache.nifi.web.api.entity.ProcessorEntity;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.apache.nifi.web.api.entity.ScheduleComponentsEntity.STATE_DISABLED;
import static org.apache.nifi.web.api.entity.ScheduleComponentsEntity.STATE_ENABLED;

@Component
public class NifiStart {
    @Autowired
    private NifiService nifiService;

    @Bean
    public CommandLineRunner run() {
        return args -> {
            startUp();
        };
    }

    public void setUp() {

        // *************************** CREATE CONTROLLER SERVICE *************************** //

        ControllerServiceEntity mySqlDbcp125Test = nifiService.addControllerService(
                new ControllerServiceBuilder()
                        .name("MYSQL_125_DBCP_TEST")
                        .type("org.apache.nifi.dbcp.DBCPConnectionPool")
                        .state("DISABLED")
                        .addProperty("Database Connection URL", "jdbc:mysql://192.168.1.125:3306/test?useUnicode=true&characterEncoding=utf-8")
                        .addProperty("Database Driver Class Name", "com.mysql.jdbc.Driver")
                        .addProperty("database-driver-locations", "/usr/local/nifi/nifi-standalone/nifi-1.7.1/lib/mysql-connector-java-5.1.34.jar")
                        .addProperty("Database User", "root")
                        .addProperty("Password", "123456")
                        .build()

        );

        ControllerServiceEntity mySqlDbcp125Test1 = nifiService.addControllerService(
                new ControllerServiceBuilder()
                        .name("MYSQL_125_DBCP_TEST1")
                        .type("org.apache.nifi.dbcp.DBCPConnectionPool")
                        .state("DISABLED")
                        .addProperty("Database Connection URL", "jdbc:mysql://192.168.1.125:3306/test1?useUnicode=true&characterEncoding=utf-8")
                        .addProperty("Database Driver Class Name", "com.mysql.jdbc.Driver")
                        .addProperty("database-driver-locations", "/usr/local/nifi/nifi-standalone/nifi-1.7.1/lib/mysql-connector-java-5.1.34.jar")
                        .addProperty("Database User", "root")
                        .addProperty("Password", "123456")
                        .build()
        );
        // *************************** GET LOCATION KEYS *************************** //

        ProcessorEntity tableFetch = nifiService.addProcessor(
                new ProcessorBuilder()
                        .name("tableFetch")
                        .type("org.apache.nifi.processors.standard.QueryDatabaseTable")
                        .position(new PositionDTO(1000.00, 1000.00))
                        .addConfigProperty("Database Connection Pooling Service", "5b515071-0165-1000-dd90-a2920d8ab972")
                        .addConfigProperty("Database Type", "MySQL")
                        .addConfigProperty("Table Name", "age")
                        .addConfigProperty("Columns to Return", "id,name")
                        .addConfigProperty("Maximum-value Columns", "id")
                        .addConfigProperty("Additional WHERE clause", "id > 1")
                        .autoTerminateAt("failure")
                        .build()
        );

        ProcessorEntity avroToJson = nifiService.addProcessor(
                new ProcessorBuilder()
                        .name("avroToJson")
                        .type("org.apache.nifi.processors.avro.ConvertAvroToJSON")
                        .position(PositionUtil.belowOf(tableFetch))
                        .autoTerminateAt("failure")
                        .build()
        );

        ProcessorEntity jsonToSql = nifiService.addProcessor(
                new ProcessorBuilder()
                        .name("jsonToSql")
                        .type("org.apache.nifi.processors.standard.ConvertJSONToSQL")
                        .position(PositionUtil.belowOf(avroToJson))
                        .addConfigProperty("JDBC Connection Pool", "5b51508f-0165-1000-11aa-11111fb5bb92")
                        .addConfigProperty("Statement Type", "INSERT")
                        .addConfigProperty("Table Name", "name")
                        .addConfigProperty("Translate Field Names", "true")
                        .addConfigProperty("Unmatched Field Behavior", "Ignore Unmatched Fields")
                        .addConfigProperty("Unmatched Column Behavior", "Fail on Unmatched Columns")
                        .autoTerminateAt("failure")
                        .autoTerminateAt("original")
                        .build()
        );

        ProcessorEntity jsonIntoDatabase = nifiService.addProcessor(
                new ProcessorBuilder()
                        .name("Insert data into database")
                        .type("org.apache.nifi.processors.standard.PutSQL")
                        .position(PositionUtil.belowOf(jsonToSql))
                        .addConfigProperty("JDBC Connection Pool", "5b51508f-0165-1000-11aa-11111fb5bb92")
                        .addConfigProperty("Support Fragmented Transactions", "false")
                        .autoTerminateAt("failure")
                        .autoTerminateAt("retry")
                        .autoTerminateAt("success")
                        .build()
        );

        // *************************** CREATE CONNECTIONS *************************** //

        // location keys
        nifiService.connectProcessors(tableFetch, avroToJson, "success");
        nifiService.connectProcessors(avroToJson, jsonToSql, "success");
        nifiService.connectProcessors(jsonToSql, jsonIntoDatabase, "sql");


        // nifiService.startProcess(tableFetch.setStatus(""));



    }
    public void startUp() {
        // start dbcp pool
        // String id,String state,ControllerServiceEntity controllerServiceEntity
        // 5b51508f-0165-1000-11aa-11111fb5bb92
/*
        // 5b515071-0165-1000-dd90-a2920d8ab972
        ControllerServiceEntity controllerServiceEntity1 =  nifiService.getControllerServiceInfo("5b51508f-0165-1000-11aa-11111fb5bb92");

        // 404 无组件
        ControllerServiceEntity controllerServiceEntity2 =  nifiService.getControllerServiceInfo("5b515071-0165-1000-dd90-a2920d8ab972");

        if (!controllerServiceEntity1.getComponent().getState().equals(STATE_ENABLED)){
            nifiService.updateControllerService(STATE_ENABLED,controllerServiceEntity1);
        }
        if (!controllerServiceEntity2.getComponent().getState().equals(STATE_ENABLED)){
            nifiService.updateControllerService(STATE_ENABLED,controllerServiceEntity2);
        }
*/

        // config dbcp pool
        ControllerServiceEntity controllerServiceEntity1 = nifiService.getControllerServiceInfo("5b51508f-0165-1000-11aa-11111fb5bb92");

        Set<ControllerServiceReferencingComponentEntity> csrs = controllerServiceEntity1.getComponent().getReferencingComponents();
        for (ControllerServiceReferencingComponentEntity csr : csrs) {
            ProcessorEntity processorEntity = nifiService.getProcessorInfo(csr.getComponent().getId());
            if (processorEntity.getComponent().getState().equals("RUNNING")) {
                System.out.println(nifiService.stopOrStartProcessor("STOPPED", processorEntity));
            }
        }


        if (controllerServiceEntity1.getComponent().getState().equals(STATE_ENABLED)) {
            System.out.println(nifiService.stopOrStartControllerService(STATE_DISABLED, controllerServiceEntity1));
        }
        Map<String, Object> propertiesVar = new HashMap<>();
        propertiesVar.put("Database Connection URL", "jdbc:mysql://192.168.1.7:3306/test?useUnicode=true&characterEncoding=utf-8");
        nifiService.updateControlServiceConfig(controllerServiceEntity1, propertiesVar);


        //config processer
        // allowableValues = "RUNNING, STOPPED, DISABLED"
        ProcessorEntity processorEntity1 = nifiService.getProcessorInfo("");
        if (processorEntity1.getComponent().getState().equals("RUNNING")) {
            nifiService.stopOrStartProcessor("STOPPED", processorEntity1);
        }
        Map<String, Object> propertiesOfProcessorVar = new HashMap<>();
        propertiesVar.put("Table Name", "name");
        nifiService.updateProcessorsCofing(processorEntity1, propertiesOfProcessorVar);

    }
}
