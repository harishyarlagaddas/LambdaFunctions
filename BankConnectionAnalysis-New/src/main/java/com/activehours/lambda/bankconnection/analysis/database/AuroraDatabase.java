package com.activehours.lambda.bankconnection.analysis.database;

import com.activehours.lambda.bankconnection.analysis.BankConnectionAnalysis;
import com.activehours.lambda.bankconnection.analysis.Model.BankConnectionEvent;
import com.activehours.lambda.bankconnection.analysis.Model.aurora.BankConnectionAnalysisEvent;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import org.joda.time.DateTime;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class AuroraDatabase {
    private static String AURORA_CONNECT_URL = "jdbc:mysql://analytics.cluster-cdtcdbnrnelx.us-west-2.rds.amazonaws.com:3306/ah_analytics";
    private static String AURORA_USER_NAME = "ah_analytics";
    private static String AURORA_PASSWORD = "ah_analyt1c5";

    private Connection mAuroraConnection = null;
    private LambdaLogger mLogger = null;

    public AuroraDatabase(LambdaLogger logger) {
        try {
            mLogger = logger;
            mLogger.log("Registering JDBC Driver");
            Class.forName("com.mysql.jdbc.Driver");
            mLogger.log("Registration of JDBC Driver completed.");
            mAuroraConnection = DriverManager.getConnection(AURORA_CONNECT_URL,AURORA_USER_NAME,AURORA_PASSWORD);
            mLogger.log("Got the connection from aurora database");
        } catch (SQLException e) {
            logger.log("Exception while getting the aurora database connection. Exception: "+e.getMessage());
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            logger.log("Exception while registering the jdbc driver. Exception: "+e.getMessage());
            e.printStackTrace();
        }
    }

    public boolean CreateAnalysisEvent(BankConnectionAnalysisEvent event) {

        // set created and last updated timestamp
        event.setCreatedAt(new Timestamp(System.currentTimeMillis()));
        event.setUpdatedAt(new Timestamp(System.currentTimeMillis()));

        try {
            PreparedStatement createStmt = event.getPreparedStatementForInsert(mAuroraConnection);
            boolean res = createStmt.execute();
            mLogger.log("Successfully inserted Analysis Event.");
            return true;
        } catch (SQLException e) {
            mLogger.log("Exception while inserting the Analysis Event. Exception: "+e.getMessage());
            e.printStackTrace();
        }
        return false;
    }

    public boolean UpdateAnalysisEvent(BankConnectionAnalysisEvent event) {

        // Set last updated time stamp
        event.setUpdatedAt(new Timestamp(System.currentTimeMillis()));

        try {
            PreparedStatement updateStmt = event.getPreparedStatementForUpdate(mAuroraConnection);
            updateStmt.execute();
            mLogger.log("Successfully Updated Analysis Event.");
            return true;
        } catch (SQLException e) {
            mLogger.log("Exception while updating the Analysis Event. Exception: "+e.getMessage());
            e.printStackTrace();
        }
        return false;
    }

    public List<BankConnectionAnalysisEvent> ReadAnalysisEvents(long userId, long financialInstitutionId, int provider) {
        List<BankConnectionAnalysisEvent> eventList = new ArrayList<>();
        BankConnectionAnalysisEvent queryEvent = new BankConnectionAnalysisEvent();
        queryEvent.setUserId(userId);
        queryEvent.setFinancialInsId(financialInstitutionId);
        queryEvent.setProvider(provider);
        try {
            PreparedStatement queryStmt = queryEvent.GetPreparedStatementForQuery(mAuroraConnection);

            ResultSet resultSet = queryStmt.executeQuery();
            while (resultSet.next()) {
                BankConnectionAnalysisEvent analysisEvent = new BankConnectionAnalysisEvent();
                analysisEvent.setId(
                        resultSet.getInt(BankConnectionAnalysisEvent.COLUMN_NAME_ID));
                analysisEvent.setUserId(
                        resultSet.getLong(BankConnectionAnalysisEvent.COLUMN_NAME_USER_ID));
                analysisEvent.setFinancialInsId(
                        resultSet.getLong(BankConnectionAnalysisEvent.COLUMN_NAME_FINANCIAL_INS_ID));
                analysisEvent.setProvider(
                        resultSet.getInt(BankConnectionAnalysisEvent.COLUMN_NAME_PROVIDER));
                analysisEvent.setConnectionState(
                        resultSet.getString(BankConnectionAnalysisEvent.COLUMN_NAME_CONNECTION_STATE));
                analysisEvent.setCreateConnCount(
                        resultSet.getInt(BankConnectionAnalysisEvent.COLUMN_NAME_CREATE_CONN_COUNT));
                analysisEvent.setUpdateConnCount(
                        resultSet.getInt(BankConnectionAnalysisEvent.COLUMN_NAME_UPDATE_CONN_COUNT));
                analysisEvent.setMfaCount(
                        resultSet.getInt(BankConnectionAnalysisEvent.COLUMN_NAME_MFA_COUNT));
                analysisEvent.setFinancialInsName(
                        resultSet.getString(BankConnectionAnalysisEvent.COLUMN_NAME_FINANCIAL_INS_NAME));
                analysisEvent.setErrorCode(
                        resultSet.getString(BankConnectionAnalysisEvent.COLUMN_NAME_ERROR_CODE));
                analysisEvent.setErrorDescription(
                        resultSet.getString(BankConnectionAnalysisEvent.COLUMN_NAME_ERROR_DESC));
                analysisEvent.setHttpStatusCode(
                        resultSet.getLong(BankConnectionAnalysisEvent.COLUMN_NAME_HTTP_STATUS_CODE));
                analysisEvent.setCreatedAt(
                        resultSet.getTimestamp(BankConnectionAnalysisEvent.COLUMN_NAME_CREATED_AT));
                analysisEvent.setUpdatedAt(
                        resultSet.getTimestamp(BankConnectionAnalysisEvent.COLUMN_NAME_UPDATED_AT));
                eventList.add(analysisEvent);
            }
            return eventList;
        } catch (SQLException e) {
            mLogger.log("Exception while getting the analysis event from database. Exception: "+e.getMessage());
            e.printStackTrace();
        }
        return eventList;
    }
}
