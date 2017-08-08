package com.activehours.lambda.bankconnection.analysis.Analyzers;

import com.activehours.lambda.bankconnection.analysis.Model.Analyzed.BankConnectionAnalysisEvent;
import com.activehours.lambda.bankconnection.analysis.Model.Incoming.*;
import com.activehours.lambda.bankconnection.analysis.S3Uploader;
import com.amazonaws.services.lambda.runtime.LambdaLogger;

import java.sql.Connection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ConnectionAnalyzer implements Analyzer {
    // Production Environment
    static String PRODUCTION_DESTINATION_DATA_ATHENA_TABLE_NAME_1 =
            "bankconnection_analysis_db.bankconnection_analysis_prod_1";
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

    public ConnectionAnalyzer(boolean isProductionEnv, LambdaLogger logger) {
        mProductionEnvironment = isProductionEnv;
        mLogger = logger;
        mS3Uploader = new S3Uploader(getAthenaTableName(), getS3BucketName(), mLogger);
    }

    @Override
    public void AnalyzeEvents(List<IncomingBankConnectionEvent> events,
                              Connection connection, LambdaLogger logger) {
        // Get the overall connection stats including all the financial institutions.
        analyzeBasedOnFinancialInsId(events, -1, logger);

        //Get connections stats for each of the financial institution id
        List<Integer> financialInsIds = events.stream().map(
                IncomingBankConnectionEvent::getFinancialInstitutionId)
                .distinct().collect(Collectors.toList());
        for (int financialInsId : financialInsIds) {
            analyzeBasedOnFinancialInsId(events, financialInsId, logger);
        }
        mS3Uploader.FinalizeAndAddPartition(connection);
    }

    private String getAthenaTableName() {
        if (mProductionEnvironment) {
            return PRODUCTION_DESTINATION_DATA_ATHENA_TABLE_NAME_1;
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

    private void analyzeBasedOnFinancialInsId(
            List<IncomingBankConnectionEvent> events, int finanacialInstitutionId,
            LambdaLogger logger) {

        for(ProviderType provider: ProviderType.values()) {
            //logger.log("Getting the connection analysis for provider: "+provider);
            BankConnectionAnalysisEvent overallAnalysisEvent = new BankConnectionAnalysisEvent();
            overallAnalysisEvent.EventType = BankConnectionEventType.ConnectionEvent.getVal();
            overallAnalysisEvent.ProviderType = provider.getVal();
            overallAnalysisEvent.FinancialInstitutionId = finanacialInstitutionId;
            List<IncomingBankConnectionEvent> eventList = events;
            if (0 < finanacialInstitutionId) {
                Stream<IncomingBankConnectionEvent> eventStream = eventList.stream()
                        .filter(event -> event.FinancialInstitutionId == finanacialInstitutionId);
                eventList = eventStream.collect(Collectors.toList());
            }

            Stream<IncomingBankConnectionEvent> eventStream = eventList.stream()
                    .filter(event -> event.BankFeedProvider == provider.getVal());
            eventList = eventStream.collect(Collectors.toList());

            if (eventList.size() > 0) {
                for (ConnectionType connectionType : ConnectionType.values()) {
                    // Total Connections...
                    Stream<IncomingBankConnectionEvent> totalCreateConnections =
                            eventList.stream().filter(event -> event.ConnectionType == connectionType.getVal());
                    long size = totalCreateConnections.count();
                    overallAnalysisEvent.setTotalConnections(connectionType, size);

                    //Successful Connections
                    Stream<IncomingBankConnectionEvent> successfulConnections =
                            eventList.stream().filter(event -> event.ConnectionType == connectionType.getVal()
                                    && event.Status == ConnectionStatus.Success.getVal());
                    size = successfulConnections.count();
                    overallAnalysisEvent.setSuccessfulConnections(connectionType, size);

                    //Failed Connections
                    Stream<IncomingBankConnectionEvent> failureConnections =
                            eventList.stream().filter(event -> event.ConnectionType == connectionType.getVal()
                                    && event.Status == ConnectionStatus.Failure.getVal());
                    size = failureConnections.count();
                    overallAnalysisEvent.setFailureConnections(connectionType, size);

                    //Non Deterministic Connections
                    Stream<IncomingBankConnectionEvent> nonDeterministicConnections =
                            eventList.stream().filter(event -> event.ConnectionType == connectionType.getVal()
                                    && event.Status == ConnectionStatus.PendingDeterministicStatus.getVal());
                    size = nonDeterministicConnections.count();
                    overallAnalysisEvent.setNonDeterministicConnections(connectionType, size);

                    //MFA Connections
                    Stream<IncomingBankConnectionEvent> mfaConnections =
                            eventList.stream().filter(event -> event.ConnectionType == connectionType.getVal()
                                    && event.Status == ConnectionStatus.Mfa.getVal());
                    size = mfaConnections.count();
                    overallAnalysisEvent.setMfaConnections(connectionType, size);
                }
            }
            mS3Uploader.uploadEvent(overallAnalysisEvent);
        }
    }
}
