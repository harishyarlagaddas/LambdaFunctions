package com.activehours.lambda.bankconnection.analysis;

import com.activehours.lambda.bankconnection.analysis.Model.BankConnectionEvent;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
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

	public HourlyBankConnectionAnalysis() {

    }

    public static String getSourceDataAthenaTableName() {
    	if (mProductionEnvironment) {
    		return PRODUCTION_SOURCE_DATA_ATHENA_TABLE_NAME;
		} else {
    		return TEST_SOURCE_DATA_ATHENA_TABLE_NAME;
		}
	}

	public static String getSourceDataS3BucketName() {
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

			UserEventAggregator analyzer = new UserEventAggregator(connection,mProductionEnvironment, logger);
			analyzer.Analyze();
        } catch (SQLException e) {
            logger.log("Exception while getting the Athena connection. Exception: "+e.getMessage());
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
	            logger.log("Exception while setting the athena driver to get Athen connection. Exception: "
						+e.getMessage());
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
}
