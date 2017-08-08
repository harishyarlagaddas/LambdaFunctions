package com.activehours.lambda.bankconnection.analysis.Analyzers;

import com.activehours.lambda.bankconnection.analysis.Model.Analyzed.BankConnectionAnalysisEvent;
import com.activehours.lambda.bankconnection.analysis.Model.Analyzed.BankConnectionErrorEvent;
import com.activehours.lambda.bankconnection.analysis.Model.Incoming.BankConnectionEventType;
import com.activehours.lambda.bankconnection.analysis.Model.Incoming.ConnectionType;
import com.activehours.lambda.bankconnection.analysis.Model.Incoming.IncomingBankConnectionEvent;
import com.activehours.lambda.bankconnection.analysis.Model.Incoming.ProviderType;
import com.activehours.lambda.bankconnection.analysis.S3Uploader;
import com.amazonaws.services.lambda.runtime.LambdaLogger;

import java.sql.Connection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ConnectionStateAnalyzer implements Analyzer {
    // Production Environment
    static String PRODUCTION_DESTINATION_DATA_ATHENA_TABLE_NAME =
            "bankconnection_analysis_db.bankconnection_error_analysis_1";
    static String PRODUCTION_DESTINATION_DATA_S3_BUCKET_NAME =
            "ah-firehose-bankconnection-analysis-prod";

    //Test environment
    static String TEST_DESTINATION_DATA_ATHENA_TABLE_NAME =
            "bankconnection_analysis_test_db.bankconnection_analysis_test_2";
    static String TEST_DESTINATION_DATA_S3_BUCKET_NAME =
            "ah-firehose-bankconnection-analysis-test";

    private S3Uploader mS3Uploader;
    private boolean mProductionEnvironment;
    private LambdaLogger mLogger;

    public ConnectionStateAnalyzer(boolean isProductionEnv, LambdaLogger logger) {
        mProductionEnvironment = isProductionEnv;
        mLogger = logger;
        mS3Uploader = new S3Uploader(getAthenaTableName(), getS3BucketName(), mLogger);
    }

    @Override
    public void AnalyzeEvents(
            List<IncomingBankConnectionEvent> events,
            Connection connection, LambdaLogger logger) {

        //Get connections stats for each of the financial institution id
        List<Integer> financialInsIds = events.stream().map(
                IncomingBankConnectionEvent::getFinancialInstitutionId)
                .distinct().collect(Collectors.toList());

        List<String> connectionStates = events.stream().map(
                IncomingBankConnectionEvent::getConnectionState).distinct().collect(Collectors.toList());
        for (String connectionState : connectionStates) {
            if (null == connectionState) {
                continue;
            }
            // Overall error codes..
            analyzeConnectionState(events, connectionState, -1, logger);

            for (int financialInsId : financialInsIds) {
                analyzeConnectionState(events, connectionState, financialInsId, logger);
            }
        }
        mS3Uploader.FinalizeAndAddPartition(connection);
    }

    private String getAthenaTableName() {
        if (mProductionEnvironment) {
            return PRODUCTION_DESTINATION_DATA_ATHENA_TABLE_NAME;
        } else {
            return TEST_DESTINATION_DATA_ATHENA_TABLE_NAME;
        }
    }

    private String getS3BucketName() {
        if (mProductionEnvironment) {
            return PRODUCTION_DESTINATION_DATA_S3_BUCKET_NAME;
        } else {
            return TEST_DESTINATION_DATA_S3_BUCKET_NAME;
        }
    }

    protected void analyzeConnectionState(
            List<IncomingBankConnectionEvent> events, String errorCode, int financialInsId,
            LambdaLogger logger) {

        for(ProviderType provider: ProviderType.values()) {
            //logger.log("Getting the Error analysis for ErrorCode: "+errorCode + " provider: " + provider+" and FinancialInsID: "+financialInsId);
            BankConnectionErrorEvent errorEvent = new BankConnectionErrorEvent();
            errorEvent.ProviderType = provider.getVal();
            errorEvent.FinancialInstitutionId = financialInsId;
            errorEvent.ErrorCode = errorCode;
            List<IncomingBankConnectionEvent> eventList = events;

            if (0 < financialInsId) {
                Stream<IncomingBankConnectionEvent> eventStream = eventList.stream()
                        .filter(event -> event.FinancialInstitutionId == financialInsId);
                eventList = eventStream.collect(Collectors.toList());
            }

            Stream<IncomingBankConnectionEvent> eventStream = eventList.stream()
                    .filter(event -> event.BankFeedProvider == provider.getVal());
            eventList = eventStream.collect(Collectors.toList());


            if (eventList.size() > 0) {
                // Total error count..
                Stream<IncomingBankConnectionEvent> errorCount;
                errorCount = eventList.stream()
                        .filter(event -> errorCode.equals(event.ConnectionState));
                errorEvent.EventType = BankConnectionEventType.ConnectionStatusEvent.getVal();

                long size = errorCount.count();
                //logger.log("Total Number of errors: " + size + " with provider: " + provider);
                errorEvent.ErrorCount = size;

                // Total create connections..
                Stream<IncomingBankConnectionEvent> totalCreateConnections =
                        eventList.stream().filter(event -> event.ConnectionType == ConnectionType.Create.getVal());
                size = totalCreateConnections.count();
                //logger.log("Total Number of create connections " + size + " with provider: " + provider);
                errorEvent.TotalNumberOfCreateConnections = size;

                // Total update connections..
                Stream<IncomingBankConnectionEvent> totalUpdateConnections =
                        eventList.stream().filter(event -> event.ConnectionType == ConnectionType.Update.getVal());
                size = totalUpdateConnections.count();
                //logger.log("Total Number of update connections " + size + " with provider: " + provider);
                errorEvent.TotalNumberOfUpdateConnections = size;

                // Total mfa connections..
                Stream<IncomingBankConnectionEvent> totalMfaConnections =
                        eventList.stream().filter(event -> event.ConnectionType == ConnectionType.Mfa.getVal());
                size = totalMfaConnections.count();
                //logger.log("Total Number of Mfa connections " + size + " with provider: " + provider);
                errorEvent.TotalNumberOfSubmittingMfaAnswers = size;
            }
            //SendEventToKinesisFirehose(errorEvent,logger);
            mS3Uploader.uploadEvent(errorEvent);
        }
    }
}
