package com.amazonaws.lambda.athena_partitions;

import java.math.RoundingMode;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DecimalFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.TimeZone;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;

public class AddAthenaPartitions implements RequestHandler<Object, String> {

	private static final String ENVIROMENTAL_VARIABLE_FOR_TABLE_NAMES = "Lambda_TablesAndPartitoins";
	@Override
    public String handleRequest(Object input, Context context) {
        LambdaLogger logger = context.getLogger();
       
        logger.log("AthenaPartitions IN. Input: "+input.toString());
        Map<String, String> tables = getTableNames(logger);
        
        for (Entry<String, String> table: tables.entrySet()) {
        	logger.log("Adding Partition for table: "+table.getKey()+" with bucket: "+table.getValue());
        	AddPartitions(table.getKey(), table.getValue(), logger);
        }
        return "Success";
    }
	
    private void AddPartitions(String tableName, String bucketName, LambdaLogger logger) {
    	
       logger.log("AddPartitions IN");
       Properties info = new Properties();
       info.put("s3_staging_dir", "s3://ah-lambda-prod/parititions/");
       info.put("aws_credentials_provider_class","com.amazonaws.auth.EnvironmentVariableCredentialsProvider");
       
       Connection connection = null;
       try {
             Class.forName("com.amazonaws.athena.jdbc.AthenaDriver");
             connection = DriverManager.getConnection("jdbc:awsathena://athena.us-west-2.amazonaws.com:443/", info);
             logger.log("Got the Athena Connection\n");
             
             Calendar cal = Calendar.getInstance();
             cal.setTimeZone(TimeZone.getTimeZone("America/New_York"));
             cal.setTime(new Date());
             
             DecimalFormat mFormat= new DecimalFormat("00");
             mFormat.setRoundingMode(RoundingMode.DOWN);
             
             int year = cal.get(Calendar.YEAR);
             String month = mFormat.format(cal.get(Calendar.MONTH)+1);
             String day = mFormat.format(cal.get(Calendar.DAY_OF_MONTH));
             String hour = mFormat.format(cal.get(Calendar.HOUR_OF_DAY));
             
             /*StringBuilder yearStatementBuilder = new StringBuilder();
             yearStatementBuilder.append("ALTER TABLE ").append(tableName)
		             .append(" ADD IF NOT EXISTS PARTITION (year=").append(year)
		             .append(") LOCATION \"s3://").append(bucketName).append("/")
		             .append(year).append("/")
		             .append("\"");
			String yearStatement = yearStatementBuilder.toString();
			executeAthenaStatement(connection, yearStatement, logger);
             
             StringBuilder monthStatementBuilder = new StringBuilder();
             monthStatementBuilder.append("ALTER TABLE ").append(tableName)
                      .append(" ADD IF NOT EXISTS PARTITION (year=").append(year)
                      .append(", month=").append(month)
                      .append(") LOCATION \"s3://").append(bucketName).append("/")
                      .append(year).append("/")
                      .append(month).append("/")
                      .append("\"");
             String monthStatement = monthStatementBuilder.toString();
             executeAthenaStatement(connection, monthStatement, logger); */
             
             StringBuilder dayStatementBuilder = new StringBuilder();
             dayStatementBuilder.append("ALTER TABLE ").append(tableName)
                      .append(" ADD IF NOT EXISTS PARTITION (year=").append(year)
                      .append(", month=").append(month)
                      .append(", day=").append(day)
                      .append(") LOCATION \"s3://").append(bucketName).append("/")
                      .append(year).append("/")
                      .append(month).append("/")
                      .append(day).append("/")
                      .append("\"");
             String dayStatement = dayStatementBuilder.toString();
             executeAthenaStatement(connection, dayStatement, logger);
             
             /*StringBuilder hourStatementBuilder = new StringBuilder();
             hourStatementBuilder.append("ALTER TABLE ").append(tableName)
                      .append(" ADD IF NOT EXISTS PARTITION (year=").append(year)
                      .append(", month=").append(month)
                      .append(", day=").append(day)
                      .append(", hour=").append(hour)
                      .append(") LOCATION \"s3://").append(bucketName).append("/")
                      .append(year).append("/")
                      .append(month).append("/")
                      .append(day).append("/")
                      .append(hour).append("/")
                      .append("\"");
                     
               String hourStatement = hourStatementBuilder.toString();
               executeAthenaStatement(connection, hourStatement, logger);*/
              } catch (SQLException e) {
                     logger.log("Exception while getting the Athena connection. Exception: "+e.getMessage());
                     e.printStackTrace();
              } catch (ClassNotFoundException e) {
                     logger.log("Exception while setting the athena driver to get Athen connection. Exception: "+e.getMessage());
                     e.printStackTrace();
              } finally {
                 try {
                     if (connection != null) {
                       connection.close();
                     }
                 } catch (Exception ex) {
                     logger.log("Exception while closing the connection.");
                     ex.printStackTrace();
                 }
           }
       logger.log("AddPartitions OUT");
    }
   
    private void executeAthenaStatement(Connection connection, String sqlCommand, LambdaLogger logger) 
    		throws SQLException {
    	logger.log("SQL Statement: "+sqlCommand);
        Statement statement = connection.createStatement();
        boolean result = statement.execute(sqlCommand);
        logger.log("Completed the execution of sql statement: "+sqlCommand+" Result: "+result);
        
        if (statement != null) {
            statement.close();
        }
    }
    
    private Map<String, String> getTableNames(LambdaLogger logger) {
    	HashMap<String, String> tableMap = new HashMap<String, String>();
    	String envStr = System.getenv(ENVIROMENTAL_VARIABLE_FOR_TABLE_NAMES);
    	logger.log("Getting table names and partitions. String from environemental variable: "+envStr);
    	
    	if (null != envStr) {
	    	String[] tables = envStr.split(",");
	    	
	    	for(String table: tables) {
	    		String[] partitions = table.split(":");
	    		if (partitions.length == 2) {
	    			logger.log("Adding the table: "+partitions[0]+" and partition: "+partitions[1]);
	    			tableMap.put(partitions[0], partitions[1]);
	    		} else {
	    			logger.log("Error while parsing the environmental variable for table and partitions. val: "+table);
	    		}
	    	}
    	}
    	logger.log("Number of tables and partitions are "+tableMap.size());
    	return tableMap;
    }
}