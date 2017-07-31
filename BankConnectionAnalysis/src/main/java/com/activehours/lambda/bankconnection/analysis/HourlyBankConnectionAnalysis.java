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
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Properties;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class HourlyBankConnectionAnalysis implements RequestHandler<Object, String> {

	String mAthenaTableName = "bankconnection_events_db.bankconnection_events";
    String mAthenaDataS3BucketName = "ah-firehose-bankconnection-events-test";
    String mKinesisFirehoseDeliveryStreamName = "Test_BankConnectionAnalysis";

    String mAthenaAnalysisTableName = "bankconnection_analysis_test_db.bankconnection_analysis_test_1";
    String mAthenaAnalysisS3BucketName = "ah-firehose-bankconnection-analysis-test";
	String mAddHourlyAthenaAnalysisPartitionSqlString =
			"ALTER TABLE "+mAthenaAnalysisTableName+" ADD IF NOT EXISTS PARTITION "
					+ "(year=%YY, month=%MM, day=%DD, hour=%HH) "
					+ "LOCATION \"s3://"+mAthenaAnalysisS3BucketName+"/%YY/%MM/%DD/%HH/\"";

    String mGetAllEventsSqlString = "SELECT * FROM "+mAthenaTableName+" WHERE year=%YY AND month=%MM AND day=%DD AND hour=%HH";

    String mAddHourlyPartitionSqlString = 
    		"ALTER TABLE "+mAthenaTableName+" ADD IF NOT EXISTS PARTITION "
    				+ "(year=%YY, month=%MM, day=%DD, hour=%HH) "
    				+ "LOCATION \"s3://"+mAthenaDataS3BucketName+"/%YY/%MM/%DD/%HH/\"";
    
    AmazonKinesisFirehose mFirehose = AmazonKinesisFirehoseClientBuilder.standard()
			.withEndpointConfiguration(
					new AwsClientBuilder.EndpointConfiguration(
							"https://firehose.us-west-2.amazonaws.com","us-west-2")).build();
    
    public HourlyBankConnectionAnalysis() {
    	
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
            
            
        	String hourlyPartitionSqlStr = mAddHourlyPartitionSqlString
        								   .replaceAll("%YY", year)
        								   .replaceAll("%MM", month)
        								   .replaceAll("%DD", day)
        								   .replaceAll("%HH", hour);
        	logger.log("Creating the hourly partition. hourlyPartitionSqlStr: "+hourlyPartitionSqlStr);
        	executeAthenaStatement(connection, hourlyPartitionSqlStr, logger);
        	logger.log("Creation of hourly partition succeeded.");

			String s3FolderName = s3Year+"/"+s3Month+"/"+s3Day+"/"+s3Hour+"/";
			createS3Folder(mAthenaAnalysisS3BucketName,s3FolderName);

        	// Add hourly partition to Athena Analysis table.
			String hourlyPartitionAthenaAnalysisSqlStr = mAddHourlyAthenaAnalysisPartitionSqlString
					.replaceAll("%YY", s3Year)
					.replaceAll("%MM", s3Month)
					.replaceAll("%DD", s3Day)
					.replaceAll("%HH", s3Hour);
			logger.log("Creating the hourly partition for athena analysis table. hourlyPartitionSqlStr: "+hourlyPartitionSqlStr);
			executeAthenaStatement(connection, hourlyPartitionAthenaAnalysisSqlStr, logger);
			logger.log("Creation of hourly partition for athena analysis table succeeded.");

        	String getAllEventsSqlStr = mGetAllEventsSqlString
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
			overallAnalysisEvent.ProviderType = provider.getVal();
			overallAnalysisEvent.FinancialInstitutionId = finanacialInstitutionId;
			List<BankConnectionEvent> eventList = events;
			Supplier<Stream<BankConnectionEvent>> eventStreamSupplier;
			if (0 < finanacialInstitutionId) {
				Stream<BankConnectionEvent> eventStream = eventList.stream()
						.filter(event -> event.FinancialInstitutionId == finanacialInstitutionId);
				eventList = eventStream.collect(Collectors.toList());
			}

			if (ProviderType.All != provider) {
				Stream<BankConnectionEvent> eventStream = eventList.stream()
						.filter(event -> event.BankFeedProvider == provider.getVal());
				eventList = eventStream.collect(Collectors.toList());
			}

			if (eventList.size() > 0) {
				for (ConnectionType connectionType : ConnectionType.values()) {
					// Total Connections...
					Stream<BankConnectionEvent> totalCreateConnections =
							eventList.stream().filter(event -> event.ConnectionType == connectionType.getVal());
					long size = totalCreateConnections.count();
					logger.log("Total Number of total connection state: "+connectionType+" Connections: " +size);
					overallAnalysisEvent.setTotalConnections(connectionType, size);

					//Successful Connections
					Stream<BankConnectionEvent> successfulCOnnections =
							eventList.stream().filter(event -> event.ConnectionType == connectionType.getVal()
									&& event.Status == ConnectionStatus.Success.getVal());
					size = successfulCOnnections.count();
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
					overallAnalysisEvent.setFailureConnections(connectionType, size);

					//MFA Connections
					Stream<BankConnectionEvent> mfaConnections =
							eventList.stream().filter(event -> event.ConnectionType == connectionType.getVal()
									&& event.Status == ConnectionStatus.Mfa.getVal());
					size = mfaConnections.count();
					logger.log("Total Number of Mfa connection state: "+connectionType+" Connections: " +size);
					overallAnalysisEvent.setFailureConnections(connectionType, size);
				}
			}
			SendEventToKinesisFirehose(overallAnalysisEvent, logger);
		}
	}

	private void analyzeErrorCodes(List<BankConnectionEvent> events, String errorCode, int financialInsId,
								   LambdaLogger logger) {

		for(ProviderType provider: ProviderType.values()) {
			logger.log("Getting the connection analysis for provider: " + provider);
			BankConnectionErrorEvent errorEvent = new BankConnectionErrorEvent();
			errorEvent.ProviderType = provider.getVal();
			errorEvent.FinancialInstitutionId = financialInsId;
			errorEvent.ErrorCode = errorCode;
			List<BankConnectionEvent> eventList = events;
			Supplier<Stream<BankConnectionEvent>> eventStreamSupplier;
			if (0 < financialInsId) {
				Stream<BankConnectionEvent> eventStream = eventList.stream()
						.filter(event -> event.FinancialInstitutionId == financialInsId);
				eventList = eventStream.collect(Collectors.toList());
			}

			if (ProviderType.All != provider) {
				Stream<BankConnectionEvent> eventStream = eventList.stream()
						.filter(event -> event.BankFeedProvider == provider.getVal());
				eventList = eventStream.collect(Collectors.toList());
			}

			if (eventList.size() > 0) {
				// Total error count..
				Stream<BankConnectionEvent> errorCount =
						eventList.stream().filter(event -> event.ErrorCode == errorCode);
				long size = errorCount.count();
				logger.log("Total Number of errors " + size + " with provider: " + provider + " and financial institution id: " + financialInsId);
				errorEvent.ErrorCount = size;

				// Total create connections..
				Stream<BankConnectionEvent> totalCreateConnections =
						eventList.stream().filter(event -> event.ConnectionType == ConnectionType.Create.getVal());
				size = totalCreateConnections.count();
				logger.log("Total Number of create connections " + size + " with provider: " + provider + " and financial institution id: " + financialInsId);
				errorEvent.TotalNumberOfCreateConnections = size;

				// Total update connections..
				Stream<BankConnectionEvent> totalUpdateConnections =
						eventList.stream().filter(event -> event.ConnectionType == ConnectionType.Update.getVal());
				size = totalUpdateConnections.count();
				logger.log("Total Number of update connections " + size + " with provider: " + provider + " and financial institution id: " + financialInsId);
				errorEvent.TotalNumberOfUpdateConnections = size;

				// Total mfa connections..
				Stream<BankConnectionEvent> totalMfaConnections =
						eventList.stream().filter(event -> event.ConnectionType == ConnectionType.Mfa.getVal());
				size = totalMfaConnections.count();
				logger.log("Total Number of Mfa connections " + size + " with provider: " + provider + " and financial institution id: " + financialInsId);
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

	public void createS3Folder(String bucketName, String folderName) {
		AmazonS3 client = AmazonS3ClientBuilder.defaultClient();
		// create meta-data for your folder and set content-length to 0
		ObjectMetadata metadata = new ObjectMetadata();
		metadata.setContentLength(0);

		// create empty content
		InputStream emptyContent = new ByteArrayInputStream(new byte[0]);

		// create a PutObjectRequest passing the folder name suffixed by /
		PutObjectRequest putObjectRequest = new PutObjectRequest(bucketName,
				folderName + ".temp", emptyContent, metadata);

		// send request to S3 to create folder
		client.putObject(putObjectRequest);
	}

    private void SendEventToKinesisFirehose(Object event, LambdaLogger logger) {
    	try {
    		PutRecordRequest putRecordRequest = new PutRecordRequest();
        	putRecordRequest.setDeliveryStreamName(mKinesisFirehoseDeliveryStreamName);
        	
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
