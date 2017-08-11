package com.activehours.lambda.bankconnection.analysis;

import com.activehours.lambda.bankconnection.analysis.Analyzers.*;
import com.activehours.lambda.bankconnection.analysis.Model.Incoming.IncomingBankConnectionEvent;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;

import java.math.RoundingMode;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.*;

public class HourlyBankConnectionAnalysis implements RequestHandler<Object, String> {

	private static boolean mProductionEnvironment = true;

	// Production Environment Variables..
	static String PRODUCTION_SOURCE_DATA_ATHENA_TABLE_NAME = "bankconnection_events_db.bankconnection_events_prod_2";
	static String PRODUCTION_SOURCE_DATA_S3_BUCKET_NAME = "ah-firehose-bankconnection-events-prod";

    // Test Environment Variables..
	static String TEST_SOURCE_DATA_ATHENA_TABLE_NAME = "bankconnection_events_db.bankconnection_events";
	static String TEST_SOURCE_DATA_S3_BUCKET_NAME = "ah-firehose-bankconnection-events-test";

	static String LAMBDA_STAGING_S3_BUCKET = "s3://ah-lambda-prod/bankconnection-analysis/";

	static String SOURCE_DATA_ATHENA_TABLE_GET_ALL_EVENTS_SQL_STRING =
			"SELECT * FROM (SELECT ROW_NUMBER() OVER (ORDER BY "
					+ IncomingBankConnectionEvent.COLUMN_NAME_EVENT_CREATION_TIME+") AS "
					+ IncomingBankConnectionEvent.COLUMN_NAME_ROW_NUMBER+", * FROM "
					+getSourceDataAthenaTableName()
					+" WHERE year IN ("
					+AthenaUtils.SQL_STATEMENT_PLACE_HOLDER_YEAR+") AND month IN ("
					+AthenaUtils.SQL_STATEMENT_PLACE_HOLDER_MONTH+") AND day IN ("
					+AthenaUtils.SQL_STATEMENT_PLACE_HOLDER_DAY+") AND hour IN ("
					+AthenaUtils.SQL_STATEMENT_PLACE_HOLDER_HOUR+") AND "
					+ IncomingBankConnectionEvent.COLUMN_NAME_EVENT_CREATION_TIME
					+" BETWEEN timestamp '"
					+AthenaUtils.SQL_STATEMENT_PLACE_HOLDER_FROM_TIME+"' AND timestamp '"
					+AthenaUtils.SQL_STATEMENT_PLACE_HOLDER_TO_TIME+"') AS RowConstrainedResult"
					+" WHERE "+ IncomingBankConnectionEvent.COLUMN_NAME_ROW_NUMBER+" > "
					+AthenaUtils.SQL_STATEMENT_PLACE_HOLDER_FROM_ROW_NUMBER+" AND "
					+ IncomingBankConnectionEvent.COLUMN_NAME_ROW_NUMBER+" <= "
					+AthenaUtils.SQL_STATEMENT_PLACE_HOLDER_TO_ROW_NUMBER+" ORDER BY "
					+ IncomingBankConnectionEvent.COLUMN_NAME_ROW_NUMBER;

	private static final int SOURCE_DATA_QUERY_LIMIT = 1000;

    public HourlyBankConnectionAnalysis() {

    }

    private static String getSourceDataAthenaTableName() {
    	if (mProductionEnvironment) {
    		return PRODUCTION_SOURCE_DATA_ATHENA_TABLE_NAME;
		} else {
    		return TEST_SOURCE_DATA_ATHENA_TABLE_NAME;
		}
	}

	private static String getSourceDataS3BucketName() {
    	if (mProductionEnvironment) {
    		return PRODUCTION_SOURCE_DATA_S3_BUCKET_NAME;
		} else {
    		return TEST_SOURCE_DATA_S3_BUCKET_NAME;
		}
	}

	@Override
    public String handleRequest(Object input, Context context) {
    	LambdaLogger logger = context.getLogger();
        logger.log("Input: " + input);

		Properties info = new Properties();
        info.put("s3_staging_dir", LAMBDA_STAGING_S3_BUCKET);
        info.put("aws_credentials_provider_class","com.amazonaws.auth.EnvironmentVariableCredentialsProvider");
        Connection connection = null;
        try {
        	Class.forName("com.amazonaws.athena.jdbc.AthenaDriver");
            connection = DriverManager.getConnection("jdbc:awsathena://athena.us-west-2.amazonaws.com:443/", info);
            logger.log("Got the Athena Connection\n");

            //Before accessing athena make sure to create the partitions.
			createPartitions(connection, logger);

			/*String getAllEventsSqlStr = AthenaUtils.
					FillHourlyPartitionsInSqlStatement(SOURCE_DATA_ATHENA_TABLE_GET_ALL_EVENTS_SQL_STRING);
			getAllEventsSqlStr = AthenaUtils.FillCreationTimeStampsInSqlStatement(getAllEventsSqlStr);

			List<IncomingBankConnectionEvent> events = new ArrayList<>();
			int rowCount = 0;
			do {
				int toCount = rowCount+SOURCE_DATA_QUERY_LIMIT;
				logger.log("Getting the connection events from: "+rowCount+" to: "+toCount);
				String sqlQueryStr = getAllEventsSqlStr
						.replaceAll(AthenaUtils.SQL_STATEMENT_PLACE_HOLDER_FROM_ROW_NUMBER,String.valueOf(rowCount))
						.replaceAll(AthenaUtils.SQL_STATEMENT_PLACE_HOLDER_TO_ROW_NUMBER,String.valueOf(toCount));
				events = getAllConnectionEvents(connection, sqlQueryStr, logger);

				Analyzer connectionAnalyzer = new ConnectionAnalyzer(mProductionEnvironment, logger);
				connectionAnalyzer.AnalyzeEvents(events, connection, logger);

				Analyzer errorsAnalyzer = new ErrorsAnalyzer(mProductionEnvironment, logger);
				errorsAnalyzer.AnalyzeEvents(events, connection, logger);

				Analyzer connectionStateAnalyzer = new ConnectionStateAnalyzer(mProductionEnvironment, logger);
				connectionStateAnalyzer.AnalyzeEvents(events, connection, logger);

				rowCount += events.size();
			}while (events.size() > 0); */

			AggregateUserEventsAnalyzer analyzer = new AggregateUserEventsAnalyzer(connection,mProductionEnvironment, logger);
			analyzer.Analyze();
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
        return "Success";
    }

	private void createPartitions(Connection connection, LambdaLogger logger) throws SQLException{
		Calendar calendar = Calendar.getInstance();
		calendar.add(Calendar.HOUR_OF_DAY, -1);
		AthenaUtils.addPartitionToAthenaTables(connection, getSourceDataAthenaTableName(),
				getSourceDataS3BucketName(), calendar, logger);
		calendar.add(Calendar.HOUR_OF_DAY, -1);
		AthenaUtils.addPartitionToAthenaTables(connection, getSourceDataAthenaTableName(),
				getSourceDataS3BucketName(), calendar, logger);
	}


	private List<IncomingBankConnectionEvent> getAllConnectionEvents(
			Connection connection, String sqlCommand, LambdaLogger logger)
			throws SQLException {

    	ResultSet result = AthenaUtils.executeAthenaStatement(connection, sqlCommand, logger);
		List<IncomingBankConnectionEvent> incomingBankConnectionEventList =
				AthenaUtils.ParseResultSetForBankConnectionEvents(result);
		logger.log("Total Connection Events: "+ incomingBankConnectionEventList.size());
		return incomingBankConnectionEventList;
	}
}
