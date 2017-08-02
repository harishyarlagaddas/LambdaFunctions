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
import java.util.*;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class HourlyBankConnectionAnalysis implements RequestHandler<Object, String> {

	private static boolean mProductionEnvironment = true;

	// Production Environment Variables..
	static String PRODUCTION_SOURCE_DATA_ATHENA_TABLE_NAME = "bankconnection_events_db.bankconnection_events_1";
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

    String SOURCE_DATA_ATHENA_TABLE_GET_ALL_EVENTS_SQL_STRING =
			"SELECT * FROM "+getSourceDataAthenaTableName()+" WHERE year=%YY AND month=%MM AND day=%DD AND hour=%HH";

    String SOURCE_DATA_ATHENA_TABLE_HOURLY_PARTITION_SQL_STRING =
    		"ALTER TABLE "+getSourceDataAthenaTableName()+" ADD IF NOT EXISTS PARTITION "
    				+ "(year=%YY, month=%MM, day=%DD, hour=%HH) "
    				+ "LOCATION \"s3://"+getSourceDataS3BucketName()+"/%YY/%MM/%DD/%HH/\"";
    
    AmazonKinesisFirehose mFirehose = AmazonKinesisFirehoseClientBuilder.standard()
			.withEndpointConfiguration(
					new AwsClientBuilder.EndpointConfiguration(
							"https://firehose.us-west-2.amazonaws.com","us-west-2")).build();
    
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

	private static String getDestinationDataS3BucketName() {
    	if (mProductionEnvironment) {
    		return PRODUCTION_DESTINATION_DATA_S3_BUCKET_NAME;
		} else {
    		return TEST_DESTINATION_DATA_S3_BUCKET_NAME;
		}
	}

	private static String getDesitnationDataKinesisFirehoseStream() {
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

        Calendar cal = Calendar.getInstance();
		String s3Year = String.valueOf(cal.get(Calendar.YEAR));
		String s3Month = mFormat.format(cal.get(Calendar.MONTH)+1);
		String s3Day = mFormat.format(cal.get(Calendar.DAY_OF_MONTH));
		String s3Hour = mFormat.format(cal.get(Calendar.HOUR_OF_DAY));

		// For Analysis we need to go one hour back as we haven't completely received the analysis events for this hour yet.
		cal.add(Calendar.HOUR_OF_DAY, -1);
        String year = String.valueOf(cal.get(Calendar.YEAR));
        String month = mFormat.format(cal.get(Calendar.MONTH)+1);
        String day = mFormat.format(cal.get(Calendar.DAY_OF_MONTH));
        String hour = mFormat.format(cal.get(Calendar.HOUR_OF_DAY));

        Properties info = new Properties();
        info.put("s3_staging_dir", "s3://ah-lambda-prod/bankconnection-analysis/");
        info.put("aws_credentials_provider_class","com.amazonaws.auth.EnvironmentVariableCredentialsProvider");
        Connection connection = null;
        try {
        	Class.forName("com.amazonaws.athena.jdbc.AthenaDriver");
            connection = DriverManager.getConnection("jdbc:awsathena://athena.us-west-2.amazonaws.com:443/", info);
            logger.log("Got the Athena Connection\n");
            
            
        	String hourlyPartitionSqlStr = SOURCE_DATA_ATHENA_TABLE_HOURLY_PARTITION_SQL_STRING
        								   .replaceAll("%YY", year)
        								   .replaceAll("%MM", month)
        								   .replaceAll("%DD", day)
        								   .replaceAll("%HH", hour);
        	logger.log("Creating the hourly partition. hourlyPartitionSqlStr: "+hourlyPartitionSqlStr);
        	executeAthenaStatement(connection, hourlyPartitionSqlStr, logger);
        	logger.log("Creation of hourly partition succeeded.");

			for (Map.Entry<String, String> entry : getDestinationPartitionsMap().entrySet()) {
				String tableName = entry.getKey();
				String bucketName = entry.getValue();

				String s3FolderName = s3Year + "/" + s3Month + "/" + s3Day + "/" + s3Hour + "/";
				createS3Folder(getDestinationDataS3BucketName(), s3FolderName, logger);

				// Add hourly partition to Athena Analysis table.
				String hourlyPartitionAthenaAnalysisSqlStr = DESTINATION_DATA_ATHENA_TABLE_HOURLY_PARTITION_SQL_STRING
						.replaceAll("%TABLE_NAME", tableName)
						.replaceAll("%BUCKET_NAME", bucketName)
						.replaceAll("%YY", s3Year)
						.replaceAll("%MM", s3Month)
						.replaceAll("%DD", s3Day)
						.replaceAll("%HH", s3Hour);
				logger.log("Creating the hourly partition for athena analysis table. hourlyPartitionSqlStr: " + hourlyPartitionSqlStr);
				executeAthenaStatement(connection, hourlyPartitionAthenaAnalysisSqlStr, logger);
				logger.log("Creation of hourly partition for athena analysis table succeeded.");
			}

        	String getAllEventsSqlStr = SOURCE_DATA_ATHENA_TABLE_GET_ALL_EVENTS_SQL_STRING
										.replaceAll("%YY", year)
										.replaceAll("%MM", month)
										.replaceAll("%DD", day)
										.replaceAll("%HH", hour);
        	List<BankConnectionEvent> events = getAllConnectionEvents(connection, getAllEventsSqlStr, logger);

        	// Get the overall connection stats including all the financial institutions.
			analyzeConnection(events, -1, logger);

			//Get connections stats for each of the financial institution id
			List<Integer> financialInsIds = events.stream().map(
					BankConnectionEvent::getFinancialInstitutionId).distinct().collect(Collectors.toList());
			for(int financialInsId: financialInsIds) {
				analyzeConnection(events, financialInsId, logger);
			}

			List<String> errorCodes = events.stream().map(
					BankConnectionEvent::getErrorCode).distinct().collect(Collectors.toList());
			for(String errorCode: errorCodes) {
				if (null == errorCode) {
					continue;
				}
				// Overall error codes..
				analyzeErrorCodes(events, errorCode, -1, logger);

				for(int financialInsId: financialInsIds) {
					analyzeErrorCodes(events, errorCode, financialInsId, logger);
				}
			}
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
			logger.log("Getting the connection analysis for provider: "+provider);
			BankConnectionAnalysisEvent overallAnalysisEvent = new BankConnectionAnalysisEvent();
			overallAnalysisEvent.EventType = BankConnectionEventType.ConnectionEvent.getVal();
			overallAnalysisEvent.ProviderType = provider.getVal();
			overallAnalysisEvent.FinancialInstitutionId = finanacialInstitutionId;
			List<BankConnectionEvent> eventList = events;
			Supplier<Stream<BankConnectionEvent>> eventStreamSupplier;
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
					logger.log("Total Number of total connection state: "+connectionType+" Connections: " +size);
					overallAnalysisEvent.setTotalConnections(connectionType, size);

					//Successful Connections
					Stream<BankConnectionEvent> successfulConnections =
							eventList.stream().filter(event -> event.ConnectionType == connectionType.getVal()
									&& event.Status == ConnectionStatus.Success.getVal());
					size = successfulConnections.count();
					logger.log("Total Number of successful connection state: "+connectionType+" Connections: " +size);
					overallAnalysisEvent.setSuccessfulConnections(connectionType, size);

					//Failed Connections
					Stream<BankConnectionEvent> failureConnections =
							eventList.stream().filter(event -> event.ConnectionType == connectionType.getVal()
									&& event.Status == ConnectionStatus.Failure.getVal());
					size = failureConnections.count();
					logger.log("Total Number of failure connection state: "+connectionType+" Connections: " +size);
					overallAnalysisEvent.setFailureConnections(connectionType, size);

					//Non Deterministic Connections
					Stream<BankConnectionEvent> nonDeterministicConnections =
							eventList.stream().filter(event -> event.ConnectionType == connectionType.getVal()
									&& event.Status == ConnectionStatus.PendingDeterministicStatus.getVal());
					size = nonDeterministicConnections.count();
					logger.log("Total Number of non deterministic connection state: "+connectionType+" Connections: " +size);
					overallAnalysisEvent.setNonDeterministicConnections(connectionType, size);

					//MFA Connections
					Stream<BankConnectionEvent> mfaConnections =
							eventList.stream().filter(event -> event.ConnectionType == connectionType.getVal()
									&& event.Status == ConnectionStatus.Mfa.getVal());
					size = mfaConnections.count();
					logger.log("Total Number of Mfa connection state: "+connectionType+" Connections: " +size);
					overallAnalysisEvent.setMfaConnections(connectionType, size);
				}
			}
			SendEventToKinesisFirehose(overallAnalysisEvent, logger);
		}
	}

	protected void analyzeErrorCodes(List<BankConnectionEvent> events, String errorCode, int financialInsId,
								   LambdaLogger logger) {

		for(ProviderType provider: ProviderType.values()) {
			logger.log("Getting the Error analysis for ErrorCode: "+errorCode
					+ " provider: " + provider+" and FinancialInsID: "+financialInsId);
			BankConnectionErrorEvent errorEvent = new BankConnectionErrorEvent();
			errorEvent.EventType = BankConnectionEventType.ErrorEvent.getVal();
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
				Stream<BankConnectionEvent> errorCount =
						eventList.stream().filter(event -> errorCode.equals(event.ErrorCode));
				long size = errorCount.count();
				logger.log("Total Number of errors: " + size + " with provider: " + provider);
				errorEvent.ErrorCount = size;

				// Total create connections..
				Stream<BankConnectionEvent> totalCreateConnections =
						eventList.stream().filter(event -> event.ConnectionType == ConnectionType.Create.getVal());
				size = totalCreateConnections.count();
				logger.log("Total Number of create connections " + size + " with provider: " + provider);
				errorEvent.TotalNumberOfCreateConnections = size;

				// Total update connections..
				Stream<BankConnectionEvent> totalUpdateConnections =
						eventList.stream().filter(event -> event.ConnectionType == ConnectionType.Update.getVal());
				size = totalUpdateConnections.count();
				logger.log("Total Number of update connections " + size + " with provider: " + provider);
				errorEvent.TotalNumberOfUpdateConnections = size;

				// Total mfa connections..
				Stream<BankConnectionEvent> totalMfaConnections =
						eventList.stream().filter(event -> event.ConnectionType == ConnectionType.Mfa.getVal());
				size = totalMfaConnections.count();
				logger.log("Total Number of Mfa connections " + size + " with provider: " + provider);
				errorEvent.TotalNumberOfSubmittingMfaAnswers = size;
			}
			SendEventToKinesisFirehose(errorEvent,logger);
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
			event.BankFeedProvider = result.getInt(BankConnectionEvent.COLUMN_NAME_BANKFEED_PROVIDER);
			event.BfpProvidedBankConnectionId = result.getLong(BankConnectionEvent.COLUMN_NAME_BFP_PROVIDED_BANK_CONNECTION_ID);
			event.ErrorCode = result.getString(BankConnectionEvent.COLUMN_NAME_ERROR_CODE);
			event.ErrorDescription = result.getString(BankConnectionEvent.COLUMN_NAME_ERROR_DESCRIPTION);
			event.FinancialInstitutionId = result.getInt(BankConnectionEvent.COLUMN_NAME_FINANCIAL_INSTITUTION_ID);
			event.HttpStatusCode = result.getInt(BankConnectionEvent.COLUMN_NAME_HTTP_STATUS_CODE);
			//event.IsAuthEnabled = result.getBoolean(BankConnectionEvent.COLUMN_NAME_IS_AUTH_ENABLED);
			event.RequestTime = result.getLong(BankConnectionEvent.COLUMN_NAME_REQUEST_TIME);
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

	public void createS3Folder(String bucketName, String folderName, LambdaLogger logger) {
		String key = folderName + UUID.randomUUID().toString();
    	try {
			AmazonS3 client = AmazonS3ClientBuilder.defaultClient();
			// create meta-data for your folder and set content-length to 0
			ObjectMetadata metadata = new ObjectMetadata();
			metadata.setContentLength(0);

			// create empty content
			InputStream emptyContent = new ByteArrayInputStream(new byte[0]);

			// create a PutObjectRequest passing the folder name suffixed by /
			PutObjectRequest putObjectRequest = new PutObjectRequest(bucketName,
					key, emptyContent, metadata);

			// send request to S3 to create folder
			client.putObject(putObjectRequest);
			logger.log("Successfully created S3 Folder. FolderName: " +key);
		}catch (Exception ex) {
    		logger.log("Exception while creating the S3 Folder. FolderName: "+key+" Exception: "+ex);
		}
	}

    private void SendEventToKinesisFirehose(Object event, LambdaLogger logger) {
    	try {
    		PutRecordRequest putRecordRequest = new PutRecordRequest();
        	putRecordRequest.setDeliveryStreamName(getDesitnationDataKinesisFirehoseStream());
        	
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
