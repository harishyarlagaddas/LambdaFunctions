package com.activehours.lambda.bankconnection.analysis;

import com.activehours.lambda.bankconnection.analysis.Model.*;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehose;
import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehoseClientBuilder;
import com.amazonaws.services.kinesisfirehose.model.PutRecordRequest;
import com.amazonaws.services.kinesisfirehose.model.Record;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.sql.*;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class HourlyBankConnectionAnalysis implements RequestHandler<Object, String> {

	private static boolean mProductionEnvironment = true;

	// Production Environment Variables..
	static String PRODUCTION_SOURCE_DATA_ATHENA_TABLE_NAME = "bankconnection_events_db.bankconnection_events_prod_2";
	static String PRODUCTION_SOURCE_DATA_S3_BUCKET_NAME = "ah-firehose-bankconnection-events-prod";
	static String PRODUCTION_DESTINATION_DATA_ATHENA_TABLE_NAME_1 = "bankconnection_analysis_db.bankconnection_analysis_prod_1";
	static String PRODUCTION_DESTINATION_DATA_ATHENA_TABLE_NAME_2 = "bankconnection_analysis_db.bankconnection_error_analysis_1";
	static String PRODUCTION_DESTINATION_DATA_S3_BUCKET_NAME = "ah-firehose-bankconnection-analysis-prod";
	static String PRODUCTION_ANALYSED_DATA_KINESIS_FIREHOSE_STREAM = "Prod_BankConnectionAnalysis";

    // Test Environment Variables..
	static String TEST_SOURCE_DATA_ATHENA_TABLE_NAME = "bankconnection_events_db.bankconnection_events";
	static String TEST_SOURCE_DATA_S3_BUCKET_NAME = "ah-firehose-bankconnection-events-test";
	static String TEST_DESTINATION_DATA_ATHENA_TABLE_NAME = "bankconnection_analysis_test_db.bankconnection_analysis_test_2";
	static String TEST_DESTINATION_DATA_S3_BUCKET_NAME = "ah-firehose-bankconnection-analysis-test";
	static String TEST_ANALYSED_DATA_KINESIS_FIREHOSE_STREAM = "Test_BankConnectionAnalysis";

	static String DESTINATION_DATA_ATHENA_TABLE_HOURLY_PARTITION_SQL_STRING =
			"ALTER TABLE %TABLE_NAME ADD IF NOT EXISTS PARTITION "
					+ "(year=%YY, month=%MM, day=%DD, hour=%HH) "
					+ "LOCATION \"s3://%BUCKET_NAME/%YY/%MM/%DD/%HH/\"";

	static String SOURCE_DATA_ATHENA_TABLE_GET_ALL_EVENTS_SQL_STRING =
			"SELECT * FROM (SELECT ROW_NUMBER() OVER (ORDER BY "
					+BankConnectionEvent.COLUMN_NAME_EVENT_CREATION_TIME+") AS "
					+BankConnectionEvent.COLUMN_NAME_ROW_NUMBER+", * FROM "
					+getSourceDataAthenaTableName()
					+" WHERE year IN (%YY) AND month IN (%MM) AND day IN (%DD) AND hour IN (%HH) AND "
					+BankConnectionEvent.COLUMN_NAME_EVENT_CREATION_TIME
					+" BETWEEN timestamp '%FROM_TIME' AND timestamp '%TO_TIME') AS RowConstrainedResult"
					+" WHERE "+BankConnectionEvent.COLUMN_NAME_ROW_NUMBER+" > %FROM_ROW_NUMBER AND "
					+BankConnectionEvent.COLUMN_NAME_ROW_NUMBER+" <= %TO_ROW_NUMBER ORDER BY "
					+BankConnectionEvent.COLUMN_NAME_ROW_NUMBER;

	static String SOURCE_DATA_ATHENA_TABLE_HOURLY_PARTITION_SQL_STRING =
    		"ALTER TABLE "+getSourceDataAthenaTableName()+" ADD IF NOT EXISTS PARTITION "
    				+ "(year=%YY, month=%MM, day=%DD, hour=%HH) "
    				+ "LOCATION \"s3://"+getSourceDataS3BucketName()+"/%YY/%MM/%DD/%HH/\"";

    AmazonKinesisFirehose mFirehose = AmazonKinesisFirehoseClientBuilder.standard()
			.withEndpointConfiguration(
					new AwsClientBuilder.EndpointConfiguration(
							"https://firehose.us-west-2.amazonaws.com","us-west-2")).build();

	private static final int SOURCE_DATA_QUERY_LIMIT = 1000;
    private static final int S3_OBJECTS_COUNT_TO_WRITE_TO_S3 = 2500;

    private ArrayList<Object> mS3ObjectList = new ArrayList<Object>();

    private Calendar mESTCalendar;
    private String mDestinationS3Folder;
    private AmazonS3 mS3Clinet;

    public HourlyBankConnectionAnalysis() {
    	mESTCalendar = Calendar.getInstance(TimeZone.getTimeZone("America/New_York"));
		mESTCalendar.add(Calendar.HOUR_OF_DAY,-1);

		DecimalFormat numberFormat= new DecimalFormat("00");
		numberFormat.setRoundingMode(RoundingMode.DOWN);
		mDestinationS3Folder = String.valueOf(mESTCalendar.get(Calendar.YEAR))
				+"/"
				+numberFormat.format(mESTCalendar.get(Calendar.MONTH)+1)
				+"/"
				+numberFormat.format(mESTCalendar.get(Calendar.DAY_OF_MONTH))
				+"/"
				+numberFormat.format(mESTCalendar.get(Calendar.HOUR_OF_DAY))
				+"/";

		mS3Clinet = AmazonS3ClientBuilder.defaultClient();
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

	private static String getDestinationDataS3BucketName() {
    	if (mProductionEnvironment) {
    		return PRODUCTION_DESTINATION_DATA_S3_BUCKET_NAME;
		} else {
    		return TEST_DESTINATION_DATA_S3_BUCKET_NAME;
		}
	}

	private static String getDestitnationDataKinesisFirehoseStream() {
    	if (mProductionEnvironment) {
    		return PRODUCTION_ANALYSED_DATA_KINESIS_FIREHOSE_STREAM;
		} else {
    		return TEST_ANALYSED_DATA_KINESIS_FIREHOSE_STREAM;
		}
	}

	private Map<String, String> getDestinationPartitionsMap() {
    	HashMap<String, String> partitionMap = new HashMap<String, String>();
    	if (mProductionEnvironment) {
    		partitionMap.put(PRODUCTION_DESTINATION_DATA_ATHENA_TABLE_NAME_1, PRODUCTION_DESTINATION_DATA_S3_BUCKET_NAME);
			partitionMap.put(PRODUCTION_DESTINATION_DATA_ATHENA_TABLE_NAME_2, PRODUCTION_DESTINATION_DATA_S3_BUCKET_NAME);
		} else {
    		partitionMap.put(TEST_DESTINATION_DATA_ATHENA_TABLE_NAME, TEST_DESTINATION_DATA_S3_BUCKET_NAME);
		}
		return partitionMap;
	}

    @Override
    public String handleRequest(Object input, Context context) {
    	LambdaLogger logger = context.getLogger();
        logger.log("Input: " + input);

		DecimalFormat mFormat= new DecimalFormat("00");
		mFormat.setRoundingMode(RoundingMode.DOWN);

        // For Analysis we need to go one hour back as we haven't completely received the analysis events for this hour yet.
		String[] years = new String[2], months = new String[2], days = new String[2], hours = new String[2];

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

		mESTCalendar.set(Calendar.MINUTE, 00);
		mESTCalendar.set(Calendar.SECOND, 00);
		mESTCalendar.set(Calendar.MILLISECOND, 000);
		String fromDateStr = dateFormat.format(mESTCalendar.getTime());

		mESTCalendar.set(Calendar.MINUTE, 59);
		mESTCalendar.set(Calendar.SECOND, 59);
		mESTCalendar.set(Calendar.MILLISECOND, 999);
		String toDateStr = dateFormat.format(mESTCalendar.getTime());

		Properties info = new Properties();
        info.put("s3_staging_dir", "s3://ah-lambda-prod/bankconnection-analysis/");
        info.put("aws_credentials_provider_class","com.amazonaws.auth.EnvironmentVariableCredentialsProvider");
        Connection connection = null;
        try {
        	Class.forName("com.amazonaws.athena.jdbc.AthenaDriver");
            connection = DriverManager.getConnection("jdbc:awsathena://athena.us-west-2.amazonaws.com:443/", info);
            logger.log("Got the Athena Connection\n");

            //Before accessing athena make sure to create the partitions.
			Calendar calendar = Calendar.getInstance();
			calendar.add(Calendar.HOUR_OF_DAY, -1);
			addPartitionToAthenaTables(connection, SOURCE_DATA_ATHENA_TABLE_HOURLY_PARTITION_SQL_STRING, calendar, logger);
			calendar.add(Calendar.HOUR_OF_DAY, -1);
			addPartitionToAthenaTables(connection, SOURCE_DATA_ATHENA_TABLE_HOURLY_PARTITION_SQL_STRING, calendar, logger);

			String getAllEventsSqlStr = SOURCE_DATA_ATHENA_TABLE_GET_ALL_EVENTS_SQL_STRING
										.replaceAll("%YY", years[0]+" , "+years[1])
										.replaceAll("%MM", months[0]+" , "+months[1])
										.replaceAll("%DD", days[0]+" , "+days[1])
										.replaceAll("%HH", hours[0]+" , "+hours[1])
										.replaceAll("%FROM_TIME", fromDateStr)
										.replaceAll("%TO_TIME", toDateStr);

			List<BankConnectionEvent> events = new ArrayList<>();
			int rowCount = 0;
			do {
				int toCount = rowCount+SOURCE_DATA_QUERY_LIMIT;
				logger.log("Getting the connection events from: "+rowCount+" to: "+toCount);
				String sqlQueryStr = getAllEventsSqlStr
						.replaceAll("%FROM_ROW_NUMBER",String.valueOf(rowCount))
						.replaceAll("%TO_ROW_NUMBER",String.valueOf(toCount));
				events = getAllConnectionEvents(connection, sqlQueryStr, logger);

				// Get the overall connection stats including all the financial institutions.
				analyzeConnection(events, -1, logger);

				//Get connections stats for each of the financial institution id
				List<Integer> financialInsIds = events.stream().map(
						BankConnectionEvent::getFinancialInstitutionId).distinct().collect(Collectors.toList());
				for (int financialInsId : financialInsIds) {
					analyzeConnection(events, financialInsId, logger);
				}


				// Dump whatever is in list to S3. As we finished analysing the connection events.
				accumulateS3ObjectsAndDump(null, true, logger);

				List<String> errorCodes = events.stream().map(
						BankConnectionEvent::getErrorCode).distinct().collect(Collectors.toList());
				for (String errorCode : errorCodes) {
					if (null == errorCode) {
						continue;
					}
					// Overall error codes..
					analyzeErrorCodes(events, errorCode, -1,
							BankConnectionErrorEvent.ErrorType.ProviderError, logger);

					for (int financialInsId : financialInsIds) {
						analyzeErrorCodes(events, errorCode, financialInsId,
								BankConnectionErrorEvent.ErrorType.ProviderError, logger);
					}
				}

				List<String> connectionStates = events.stream().map(
						BankConnectionEvent::getConnectionState).distinct().collect(Collectors.toList());
				for (String connectionState : connectionStates) {
					if (null == connectionState) {
						continue;
					}
					// Overall error codes..
					analyzeErrorCodes(events, connectionState, -1,
							BankConnectionErrorEvent.ErrorType.ConnectionState, logger);

					for (int financialInsId : financialInsIds) {
						analyzeErrorCodes(events, connectionState, financialInsId,
								BankConnectionErrorEvent.ErrorType.ConnectionState, logger);
					}
				}

				// Dump whatever is in list to S3. As we finished analysing the error events.
				accumulateS3ObjectsAndDump(null, true, logger);

				rowCount += events.size();
			}while (events.size() > 0);

			//After completing the analysis, now add partitions to the destination athena table.
			addPartitionToAthenaTables(connection,DESTINATION_DATA_ATHENA_TABLE_HOURLY_PARTITION_SQL_STRING,
					mESTCalendar, logger);
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

    private void analyzeConnection(List<BankConnectionEvent> events, int finanacialInstitutionId,
								   LambdaLogger logger) {

		for(ProviderType provider: ProviderType.values()) {
			//logger.log("Getting the connection analysis for provider: "+provider);
			BankConnectionAnalysisEvent overallAnalysisEvent = new BankConnectionAnalysisEvent();
			overallAnalysisEvent.EventType = BankConnectionEventType.ConnectionEvent.getVal();
			overallAnalysisEvent.ProviderType = provider.getVal();
			overallAnalysisEvent.FinancialInstitutionId = finanacialInstitutionId;
			List<BankConnectionEvent> eventList = events;
			if (0 < finanacialInstitutionId) {
				Stream<BankConnectionEvent> eventStream = eventList.stream()
						.filter(event -> event.FinancialInstitutionId == finanacialInstitutionId);
				eventList = eventStream.collect(Collectors.toList());
			}

			Stream<BankConnectionEvent> eventStream = eventList.stream()
					.filter(event -> event.BankFeedProvider == provider.getVal());
			eventList = eventStream.collect(Collectors.toList());

			if (eventList.size() > 0) {
				for (ConnectionType connectionType : ConnectionType.values()) {
					// Total Connections...
					Stream<BankConnectionEvent> totalCreateConnections =
							eventList.stream().filter(event -> event.ConnectionType == connectionType.getVal());
					long size = totalCreateConnections.count();
					//logger.log("Total Number of total connection state: "+connectionType+" Connections: " +size);
					overallAnalysisEvent.setTotalConnections(connectionType, size);

					//Successful Connections
					Stream<BankConnectionEvent> successfulConnections =
							eventList.stream().filter(event -> event.ConnectionType == connectionType.getVal()
									&& event.Status == ConnectionStatus.Success.getVal());
					size = successfulConnections.count();
					//logger.log("Total Number of successful connection state: "+connectionType+" Connections: " +size);
					overallAnalysisEvent.setSuccessfulConnections(connectionType, size);

					//Failed Connections
					Stream<BankConnectionEvent> failureConnections =
							eventList.stream().filter(event -> event.ConnectionType == connectionType.getVal()
									&& event.Status == ConnectionStatus.Failure.getVal());
					size = failureConnections.count();
					//logger.log("Total Number of failure connection state: "+connectionType+" Connections: " +size);
					overallAnalysisEvent.setFailureConnections(connectionType, size);

					//Non Deterministic Connections
					Stream<BankConnectionEvent> nonDeterministicConnections =
							eventList.stream().filter(event -> event.ConnectionType == connectionType.getVal()
									&& event.Status == ConnectionStatus.PendingDeterministicStatus.getVal());
					size = nonDeterministicConnections.count();
					//logger.log("Total Number of non deterministic connection state: "+connectionType+" Connections: " +size);
					overallAnalysisEvent.setNonDeterministicConnections(connectionType, size);

					//MFA Connections
					Stream<BankConnectionEvent> mfaConnections =
							eventList.stream().filter(event -> event.ConnectionType == connectionType.getVal()
									&& event.Status == ConnectionStatus.Mfa.getVal());
					size = mfaConnections.count();
					//logger.log("Total Number of Mfa connection state: "+connectionType+" Connections: " +size);
					overallAnalysisEvent.setMfaConnections(connectionType, size);
				}
			}
			//SendEventToKinesisFirehose(overallAnalysisEvent, logger);
			accumulateS3ObjectsAndDump(overallAnalysisEvent, false, logger);
		}
	}

	protected void analyzeErrorCodes(List<BankConnectionEvent> events, String errorCode, int financialInsId,
									 BankConnectionErrorEvent.ErrorType errorType, LambdaLogger logger) {

		for(ProviderType provider: ProviderType.values()) {
			//logger.log("Getting the Error analysis for ErrorCode: "+errorCode + " provider: " + provider+" and FinancialInsID: "+financialInsId);
			BankConnectionErrorEvent errorEvent = new BankConnectionErrorEvent();
			errorEvent.ProviderType = provider.getVal();
			errorEvent.FinancialInstitutionId = financialInsId;
			errorEvent.ErrorCode = errorCode;
			List<BankConnectionEvent> eventList = events;

			if (0 < financialInsId) {
				Stream<BankConnectionEvent> eventStream = eventList.stream()
						.filter(event -> event.FinancialInstitutionId == financialInsId);
				eventList = eventStream.collect(Collectors.toList());
			}

			Stream<BankConnectionEvent> eventStream = eventList.stream()
					.filter(event -> event.BankFeedProvider == provider.getVal());
			eventList = eventStream.collect(Collectors.toList());


			if (eventList.size() > 0) {
				// Total error count..
				Stream<BankConnectionEvent> errorCount;
				if (BankConnectionErrorEvent.ErrorType.ProviderError == errorType) {
					errorCount = eventList.stream().filter(event -> errorCode.equals(event.ErrorCode));
					errorEvent.EventType = BankConnectionEventType.ErrorEvent.getVal();
				} else if (BankConnectionErrorEvent.ErrorType.ConnectionState == errorType){
					errorCount = eventList.stream().filter(event -> errorCode.equals(event.ConnectionState));
					errorEvent.EventType = BankConnectionEventType.ConnectionStatusEvent.getVal();
				} else {
					logger.log("This error type is not known. Hence can not proceed further...");
					continue;
				}
				long size = errorCount.count();
				//logger.log("Total Number of errors: " + size + " with provider: " + provider);
				errorEvent.ErrorCount = size;

				// Total create connections..
				Stream<BankConnectionEvent> totalCreateConnections =
						eventList.stream().filter(event -> event.ConnectionType == ConnectionType.Create.getVal());
				size = totalCreateConnections.count();
				//logger.log("Total Number of create connections " + size + " with provider: " + provider);
				errorEvent.TotalNumberOfCreateConnections = size;

				// Total update connections..
				Stream<BankConnectionEvent> totalUpdateConnections =
						eventList.stream().filter(event -> event.ConnectionType == ConnectionType.Update.getVal());
				size = totalUpdateConnections.count();
				//logger.log("Total Number of update connections " + size + " with provider: " + provider);
				errorEvent.TotalNumberOfUpdateConnections = size;

				// Total mfa connections..
				Stream<BankConnectionEvent> totalMfaConnections =
						eventList.stream().filter(event -> event.ConnectionType == ConnectionType.Mfa.getVal());
				size = totalMfaConnections.count();
				//logger.log("Total Number of Mfa connections " + size + " with provider: " + provider);
				errorEvent.TotalNumberOfSubmittingMfaAnswers = size;
			}
			//SendEventToKinesisFirehose(errorEvent,logger);
			accumulateS3ObjectsAndDump(errorEvent, false, logger);
		}
	}

	private void addPartitionToAthenaTables(Connection connection, String sqlStr,
			Calendar calendar, LambdaLogger logger)
			throws SQLException {

		DecimalFormat numberFormat= new DecimalFormat("00");
		numberFormat.setRoundingMode(RoundingMode.DOWN);

		String estYear = String.valueOf(calendar.get(Calendar.YEAR));
		String estMonth = numberFormat.format(calendar.get(Calendar.MONTH)+1);
		String estDay = numberFormat.format(calendar.get(Calendar.DAY_OF_MONTH));
		String estHour = numberFormat.format(calendar.get(Calendar.HOUR_OF_DAY));

		for (Map.Entry<String, String> entry : getDestinationPartitionsMap().entrySet()) {
			String tableName = entry.getKey();
			String bucketName = entry.getValue();

			// Add hourly partition to Athena Analysis table.
			String hourlyPartitionSqlStr = sqlStr
					.replaceAll("%TABLE_NAME", tableName)
					.replaceAll("%BUCKET_NAME", bucketName)
					.replaceAll("%YY", estYear)
					.replaceAll("%MM", estMonth)
					.replaceAll("%DD", estDay)
					.replaceAll("%HH", estHour);

			logger.log("Creating the hourly partition for athena analysis table. hourlyPartitionSqlStr: " + hourlyPartitionSqlStr);
			executeAthenaStatement(connection, hourlyPartitionSqlStr, logger);
			logger.log("Creation of hourly partition for athena analysis table succeeded.");
		}
	}

    private ResultSet executeAthenaStatement(Connection connection, String sqlCommand, LambdaLogger logger) 
    		throws SQLException {
    	logger.log("Executing SQL Statement: "+sqlCommand);
        Statement statement = connection.createStatement();
        ResultSet result = statement.executeQuery(sqlCommand);
        logger.log("Completed the execution of sql statement: "+sqlCommand+" Result: "+result);
        
        if (statement != null) {
            statement.close();
        }
        return result;
    }

	private List<BankConnectionEvent> getAllConnectionEvents(Connection connection, String sqlCommand, LambdaLogger logger)
			throws SQLException {
		logger.log("Executing SQL Statement: "+sqlCommand);
		Statement statement = connection.createStatement();
		ResultSet result = statement.executeQuery(sqlCommand);
		logger.log("Completed the execution of sql statement.");

		List<BankConnectionEvent> bankConnectionEventList = new ArrayList<BankConnectionEvent>();
		while(result.next()) {
			BankConnectionEvent event = new BankConnectionEvent();
			event.ConnectionType = result.getInt(BankConnectionEvent.COLUMN_NAME_CONNECTION_TYPE);
			event.ConnectionState = result.getString(BankConnectionEvent.COLUMN_NAME_CONNECTION_STATE);
			event.BankFeedProvider = result.getInt(BankConnectionEvent.COLUMN_NAME_BANKFEED_PROVIDER);
			event.BfpProvidedBankConnectionId = result.getLong(BankConnectionEvent.COLUMN_NAME_BFP_PROVIDED_BANK_CONNECTION_ID);
			event.ErrorCode = result.getString(BankConnectionEvent.COLUMN_NAME_ERROR_CODE);
			event.ErrorDescription = result.getString(BankConnectionEvent.COLUMN_NAME_ERROR_DESCRIPTION);
			event.FinancialInstitutionId = result.getInt(BankConnectionEvent.COLUMN_NAME_FINANCIAL_INSTITUTION_ID);
			event.FinancialInstitutionName = result.getString(BankConnectionEvent.COLUMN_NAME_FINANCIAL_INSTITUTION_NAME);
			event.HttpStatusCode = result.getInt(BankConnectionEvent.COLUMN_NAME_HTTP_STATUS_CODE);
			//event.IsAuthEnabled = result.getBoolean(BankConnectionEvent.COLUMN_NAME_IS_AUTH_ENABLED);
			event.RequestTime = result.getLong(BankConnectionEvent.COLUMN_NAME_REQUEST_TIME);
			//event.EventCreationTime = result.getDate(BankConnectionEvent.COLUMN_NAME_EVENT_CREATION_TIME);
			event.Status = result.getInt(BankConnectionEvent.COLUMN_NAME_STATUS);
			event.UserId = result.getLong(BankConnectionEvent.COLUMN_NAME_USERID);
			event.UserProvidedBankConnectionId = result.getLong(BankConnectionEvent.COLUMN_NAME_USER_PROVIDED_BANK_CONNECTION_ID);

			bankConnectionEventList.add(event);
		}
		if (statement != null) {
			statement.close();
		}

		logger.log("Total Connection Events: "+bankConnectionEventList.size());
		return bankConnectionEventList;
	}

	private void accumulateS3ObjectsAndDump(Object s3Event, boolean forceDump, LambdaLogger logger){
    	if (null != s3Event) {
			mS3ObjectList.add(s3Event);
		}

    	if (mS3ObjectList. size() == S3_OBJECTS_COUNT_TO_WRITE_TO_S3 || forceDump) {
    		logger.log("Dumping to S3. ObjectList size: "+ mS3ObjectList.size()+" forceDump: "+forceDump);
    		writeToS3(mS3ObjectList, logger);
			mS3ObjectList.clear();
		}
	}

	public void writeToS3(ArrayList<Object> s3ObjectList, LambdaLogger logger) {

		if (s3ObjectList.size() <= 0) {
			logger.log("S3Object list is empty. Hence no need to push to S3");
			return;
		}

		String key = mDestinationS3Folder + UUID.randomUUID().toString();
		try {
			ObjectMapper mapper = new ObjectMapper();
			StringBuilder builder = new StringBuilder();
			for(Object s3Obj: s3ObjectList) {
				builder.append(mapper.writeValueAsString(s3Obj));
				builder.append("\n");
			}

			String s3FinalStr = builder.toString();
			ObjectMetadata metadata = new ObjectMetadata();
			metadata.setContentLength(s3FinalStr.length());

			// create empty content
			InputStream s3Content = new ByteArrayInputStream(s3FinalStr.getBytes());

			// create a PutObjectRequest passing the folder name suffixed by /
			PutObjectRequest putObjectRequest = new PutObjectRequest(getDestinationDataS3BucketName(),
					key, s3Content, metadata);

			// send request to S3 to create folder
			mS3Clinet.putObject(putObjectRequest);
			logger.log("Successfully uploaded to S3 File: " +key);
		}catch (Exception ex) {
    		logger.log("Exception while uploading to S3. FileName: "+key+" Exception: "+ex);
		}
	}

    private void SendEventToKinesisFirehose(Object event, LambdaLogger logger) {
    	try {
    		PutRecordRequest putRecordRequest = new PutRecordRequest();
        	putRecordRequest.setDeliveryStreamName(getDestitnationDataKinesisFirehoseStream());
        	
        	ObjectMapper mapper = new ObjectMapper();
        	Record record = new Record();
        	String data = mapper.writeValueAsString(event)+"\n";
			record.setData(ByteBuffer.wrap(data.getBytes()));
			putRecordRequest.setRecord(record);
			
			mFirehose.putRecord(putRecordRequest);
		} catch (IOException ex) {
			logger.log("Exception while writing the record to kinesis stream. Exception: "+ex);
			ex.printStackTrace();
		}
    	
    }
}
