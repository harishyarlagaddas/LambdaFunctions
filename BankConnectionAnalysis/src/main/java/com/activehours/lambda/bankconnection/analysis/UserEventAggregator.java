package com.activehours.lambda.bankconnection.analysis;

import com.activehours.lambda.bankconnection.analysis.Analyzers.UserEventsAnalyzer;
import com.activehours.lambda.bankconnection.analysis.Model.BankConnectionEvent;
import com.amazonaws.services.lambda.runtime.LambdaLogger;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class UserEventAggregator {
    // Production Environment Variables..
    static String PRODUCTION_DESTINATION_DATA_ATHENA_TABLE_NAME =
            "bankconnection_analysis_db.bankconnection_analysis_prod_3";
    static String PRODUCTION_DESTINATION_DATA_S3_BUCKET_NAME = "ah-firehose-bankconnection-analysis-prod";

    // Test Environment Variables..
    static String TEST_DESTINATION_DATA_ATHENA_TABLE_NAME =
            "bankconnection_analysis_db.bankconnection_analysis_test_1";
    static String TEST_DESTINATION_DATA_S3_BUCKET_NAME = "ah-firehose-bankconnection-analysis-test";

    static String ATHENA_GET_ALL_USERS_SQL_STRING =
            "SELECT DISTINCT "+ BankConnectionEvent.COLUMN_NAME_USERID
                    +" FROM "+AthenaUtils.SQL_STATEMENT_PLACE_HOLDER_TABLE_NAME
                    +" WHERE year IN ("
                    + AthenaUtils.SQL_STATEMENT_PLACE_HOLDER_YEAR+") AND month IN ("
                    +AthenaUtils.SQL_STATEMENT_PLACE_HOLDER_MONTH+") AND day IN ("
                    +AthenaUtils.SQL_STATEMENT_PLACE_HOLDER_DAY+") AND hour IN ("
                    +AthenaUtils.SQL_STATEMENT_PLACE_HOLDER_HOUR+") AND "
                    + BankConnectionEvent.COLUMN_NAME_EVENT_CREATION_TIME
                    +" BETWEEN timestamp '"
                    +AthenaUtils.SQL_STATEMENT_PLACE_HOLDER_FROM_TIME+"' AND timestamp '"
                    +AthenaUtils.SQL_STATEMENT_PLACE_HOLDER_TO_TIME+"'";

    static String ATHENA_GET_EVENTS_FOR_USERS_SQL_STRING =
            "SELECT * FROM "+AthenaUtils.SQL_STATEMENT_PLACE_HOLDER_TABLE_NAME
            +" WHERE year IN ("
            + AthenaUtils.SQL_STATEMENT_PLACE_HOLDER_YEAR+") AND month IN ("
            +AthenaUtils.SQL_STATEMENT_PLACE_HOLDER_MONTH+") AND day IN ("
            +AthenaUtils.SQL_STATEMENT_PLACE_HOLDER_DAY+") AND hour IN ("
            +AthenaUtils.SQL_STATEMENT_PLACE_HOLDER_HOUR+") AND "
            + BankConnectionEvent.COLUMN_NAME_USERID+" IN ("
            +AthenaUtils.SQL_STATEMENT_PLACE_HOLDER_USER_IDS+")";

    private static int ATHENA_EVENTS_FETCH_USER_SIZE = 50;

    private boolean mProductionEnvironment = false;
    private Connection mConnection;
    private LambdaLogger mLogger;

    public UserEventAggregator(Connection connection, boolean isProductionEnv, LambdaLogger logger) {
        mConnection = connection;
        mProductionEnvironment = isProductionEnv;
        mLogger = logger;
    }

    public void Analyze() {
        try {
            List<Long> totalUserList = retrieveUsersFromAthena();
            mLogger.log("Users: "+constructStringFromUserIds(totalUserList));
            S3Uploader s3Uploader = new S3Uploader(
                    getDestinationDataAthenaTableName(),getDestinationDataS3BucketName(), mLogger);
            UserEventsAnalyzer userEventsAnalyzer = new UserEventsAnalyzer(s3Uploader);
            int index = 0;
            do {
                int endIndex = index + ATHENA_EVENTS_FETCH_USER_SIZE;
                if (endIndex > totalUserList.size()) {
                    endIndex = totalUserList.size();
                }

                List<Long> userList = totalUserList.subList(index, endIndex);
                List<BankConnectionEvent> eventList = retrieveUserEventsFromAthena(userList);
                for (long user: userList) {
                    List<BankConnectionEvent> userEventList = filterUserEvents(eventList, user);
                    userEventsAnalyzer.AnalyzeEvents(userEventList, mConnection, mLogger);
                }
                index+=userList.size();
            }while (index < totalUserList.size());
            s3Uploader.FinalizeAndAddPartition(mConnection);
        }catch (Exception ex) {
            mLogger.log("Exception while analyzing events in UserEventAggregator. Exception: "+getStacktraceToString(ex));
        }
    }

    private String getStacktraceToString(Throwable throwable) {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        throwable.printStackTrace(pw);
        return sw.toString();
    }

    private List<BankConnectionEvent> filterUserEvents(
            List<BankConnectionEvent> eventList, long userId) {
        Stream<BankConnectionEvent> eventStream = eventList.stream()
                .filter(event -> event.UserId == userId);
        List<BankConnectionEvent> filteredList = eventStream.collect(Collectors.toList());
        mLogger.log("For user: "+userId+" EventList Count: "+filteredList.size());
        return filteredList;
    }

    private List<BankConnectionEvent> retrieveUserEventsFromAthena(
            List<Long> users) throws SQLException {

        String getEventsSqlStr = ATHENA_GET_EVENTS_FOR_USERS_SQL_STRING
                .replaceAll(AthenaUtils.SQL_STATEMENT_PLACE_HOLDER_TABLE_NAME,
                        HourlyBankConnectionAnalysis.getSourceDataAthenaTableName());

        getEventsSqlStr = AthenaUtils
                .FillHourlyPartitionsInSqlStatement(getEventsSqlStr);

        getEventsSqlStr = getEventsSqlStr.replaceAll(AthenaUtils.SQL_STATEMENT_PLACE_HOLDER_USER_IDS,
                constructStringFromUserIds(users));
        ResultSet result = AthenaUtils.executeAthenaStatement(mConnection, getEventsSqlStr, mLogger);
        List<BankConnectionEvent> eventList =
                AthenaUtils.ParseResultSetForBankConnectionEvents(result);
        return eventList;
    }

    private List<Long> retrieveUsersFromAthena() throws SQLException {
        String getAllUsersSqlStr = ATHENA_GET_ALL_USERS_SQL_STRING
                .replaceAll(AthenaUtils.SQL_STATEMENT_PLACE_HOLDER_TABLE_NAME,
                        HourlyBankConnectionAnalysis.getSourceDataAthenaTableName());

        getAllUsersSqlStr = AthenaUtils
                .FillHourlyPartitionsInSqlStatement(getAllUsersSqlStr);

        getAllUsersSqlStr = AthenaUtils.FillCreationTimeStampsInSqlStatement(getAllUsersSqlStr);

        ResultSet result = AthenaUtils.executeAthenaStatement(mConnection, getAllUsersSqlStr, mLogger);
        List<Long> userList = new ArrayList<Long>();
        while(result.next()) {
            long userId = result.getLong(BankConnectionEvent.COLUMN_NAME_USERID);
            userList.add(userId);
        }
        mLogger.log("Retrieved total users count: "+userList.size());
        return userList;
    }

    private String constructStringFromUserIds(List<Long> users) {
        StringBuilder builder = new StringBuilder();
        for(int i = 0; i < users.size(); ++i) {
            builder.append(users.get(i));
            if (i < users.size() - 1) {
                builder.append(',');
            }
        }
        return builder.toString();
    }

    private String getDestinationDataAthenaTableName() {
        if (mProductionEnvironment) {
            return PRODUCTION_DESTINATION_DATA_ATHENA_TABLE_NAME;
        } else {
            return TEST_DESTINATION_DATA_ATHENA_TABLE_NAME;
        }
    }

    private String getDestinationDataS3BucketName() {
        if (mProductionEnvironment) {
            return PRODUCTION_DESTINATION_DATA_S3_BUCKET_NAME;
        } else {
            return TEST_DESTINATION_DATA_S3_BUCKET_NAME;
        }
    }
}
