package com.activehours.lambda.bankconnection.analysis;

import com.activehours.lambda.bankconnection.analysis.Analyzers.Analyzer;
import com.activehours.lambda.bankconnection.analysis.Analyzers.ConnectionAnalyzer;
import com.activehours.lambda.bankconnection.analysis.Analyzers.ConnectionStateAnalyzer;
import com.activehours.lambda.bankconnection.analysis.Analyzers.ErrorsAnalyzer;
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
					+" WHERE year IN (%YY) AND month IN (%MM) AND day IN (%DD) AND hour IN (%HH) AND "
					+ IncomingBankConnectionEvent.COLUMN_NAME_EVENT_CREATION_TIME
					+" BETWEEN timestamp '%FROM_TIME' AND timestamp '%TO_TIME') AS RowConstrainedResult"
					+" WHERE "+ IncomingBankConnectionEvent.COLUMN_NAME_ROW_NUMBER+" > %FROM_ROW_NUMBER AND "
					+ IncomingBankConnectionEvent.COLUMN_NAME_ROW_NUMBER+" <= %TO_ROW_NUMBER ORDER BY "
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

		/* For Analysis we need to go one hour back as we haven't completely received the analysis
         * events for this hour yet. And also we need to search in previous previous hour directory also
         * because some of the events might have written to that directory because of how
         * kinesis firehose operates.
         */
		String[] years = new String[2], months = new String[2], days = new String[2], hours = new String[2];

		DecimalFormat mFormat= new DecimalFormat("00");
		mFormat.setRoundingMode(RoundingMode.DOWN);

		Calendar cal = Calendar.getInstance();
		cal.add(Calendar.HOUR_OF_DAY, -1);
        years[0] = String.valueOf(cal.get(Calendar.YEAR));
        months[0] = mFormat.format(cal.get(Calendar.MONTH)+1);
        days[0] = mFormat.format(cal.get(Calendar.DAY_OF_MONTH));
        hours[0] = mFormat.format(cal.get(Calendar.HOUR_OF_DAY));

        cal.add(Calendar.HOUR_OF_DAY, -1);
		years[1] = String.valueOf(cal.get(Calendar.YEAR));
		months[1] = mFormat.format(cal.get(Calendar.MONTH)+1);
		days[1] = mFormat.format(cal.get(Calendar.DAY_OF_MONTH));
		hours[1] = mFormat.format(cal.get(Calendar.HOUR_OF_DAY));

		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
		dateFormat.setTimeZone(TimeZone.getTimeZone("America/New_York"));

		Calendar estCalendar = Calendar.getInstance(TimeZone.getTimeZone("America/New_York"));
		estCalendar.add(Calendar.HOUR_OF_DAY,-1);

		estCalendar.set(Calendar.MINUTE, 00);
		estCalendar.set(Calendar.SECOND, 00);
		estCalendar.set(Calendar.MILLISECOND, 000);
		String fromDateStr = dateFormat.format(estCalendar.getTime());

		estCalendar.set(Calendar.MINUTE, 59);
		estCalendar.set(Calendar.SECOND, 59);
		estCalendar.set(Calendar.MILLISECOND, 999);
		String toDateStr = dateFormat.format(estCalendar.getTime());

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

			String getAllEventsSqlStr = SOURCE_DATA_ATHENA_TABLE_GET_ALL_EVENTS_SQL_STRING
										.replaceAll("%YY", years[0]+" , "+years[1])
										.replaceAll("%MM", months[0]+" , "+months[1])
										.replaceAll("%DD", days[0]+" , "+days[1])
										.replaceAll("%HH", hours[0]+" , "+hours[1])
										.replaceAll("%FROM_TIME", fromDateStr)
										.replaceAll("%TO_TIME", toDateStr);

			List<IncomingBankConnectionEvent> events = new ArrayList<>();
			int rowCount = 0;
			do {
				int toCount = rowCount+SOURCE_DATA_QUERY_LIMIT;
				logger.log("Getting the connection events from: "+rowCount+" to: "+toCount);
				String sqlQueryStr = getAllEventsSqlStr
						.replaceAll("%FROM_ROW_NUMBER",String.valueOf(rowCount))
						.replaceAll("%TO_ROW_NUMBER",String.valueOf(toCount));
				events = getAllConnectionEvents(connection, sqlQueryStr, logger);

				Analyzer connectionAnalyzer = new ConnectionAnalyzer(mProductionEnvironment, logger);
				connectionAnalyzer.AnalyzeEvents(events, connection, logger);

				Analyzer errorsAnalyzer = new ErrorsAnalyzer(mProductionEnvironment, logger);
				errorsAnalyzer.AnalyzeEvents(events, connection, logger);

				Analyzer connectionStateAnalyzer = new ConnectionStateAnalyzer(mProductionEnvironment, logger);
				connectionStateAnalyzer.AnalyzeEvents(events, connection, logger);

				rowCount += events.size();
			}while (events.size() > 0);
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
		List<IncomingBankConnectionEvent> incomingBankConnectionEventList = new ArrayList<IncomingBankConnectionEvent>();
		while(result.next()) {
			IncomingBankConnectionEvent event = new IncomingBankConnectionEvent();
			event.ConnectionType = result.getInt(IncomingBankConnectionEvent.COLUMN_NAME_CONNECTION_TYPE);
			event.ConnectionState = result.getString(IncomingBankConnectionEvent.COLUMN_NAME_CONNECTION_STATE);
			event.BankFeedProvider = result.getInt(IncomingBankConnectionEvent.COLUMN_NAME_BANKFEED_PROVIDER);
			event.BfpProvidedBankConnectionId = result.getLong(IncomingBankConnectionEvent.COLUMN_NAME_BFP_PROVIDED_BANK_CONNECTION_ID);
			event.ErrorCode = result.getString(IncomingBankConnectionEvent.COLUMN_NAME_ERROR_CODE);
			event.ErrorDescription = result.getString(IncomingBankConnectionEvent.COLUMN_NAME_ERROR_DESCRIPTION);
			event.FinancialInstitutionId = result.getInt(IncomingBankConnectionEvent.COLUMN_NAME_FINANCIAL_INSTITUTION_ID);
			event.FinancialInstitutionName = result.getString(IncomingBankConnectionEvent.COLUMN_NAME_FINANCIAL_INSTITUTION_NAME);
			event.HttpStatusCode = result.getInt(IncomingBankConnectionEvent.COLUMN_NAME_HTTP_STATUS_CODE);
			//event.IsAuthEnabled = result.getBoolean(IncomingBankConnectionEvent.COLUMN_NAME_IS_AUTH_ENABLED);
			event.RequestTime = result.getLong(IncomingBankConnectionEvent.COLUMN_NAME_REQUEST_TIME);
			//event.EventCreationTime = result.getDate(IncomingBankConnectionEvent.COLUMN_NAME_EVENT_CREATION_TIME);
			event.Status = result.getInt(IncomingBankConnectionEvent.COLUMN_NAME_STATUS);
			event.UserId = result.getLong(IncomingBankConnectionEvent.COLUMN_NAME_USERID);
			event.UserProvidedBankConnectionId = result.getLong(IncomingBankConnectionEvent.COLUMN_NAME_USER_PROVIDED_BANK_CONNECTION_ID);

			incomingBankConnectionEventList.add(event);
		}
		logger.log("Total Connection Events: "+ incomingBankConnectionEventList.size());
		return incomingBankConnectionEventList;
	}
}
