package com.activehours.lambda.bankconnection.analysis.Model.aurora;

import com.activehours.lambda.bankconnection.analysis.utils.DateTimeUtils;

import java.sql.*;

public class BankConnectionAnalysisEvent {

    public static String AURORA_BANK_CONNECTION_ANALYSIS_TABLE_NAME = "bankconnection_analysis";

    public static final String COLUMN_NAME_ID = "id";
    public static final String COLUMN_NAME_USER_ID = "user_id";
    public static final String COLUMN_NAME_FINANCIAL_INS_ID = "fin_ins_id";
    public static final String COLUMN_NAME_FINANCIAL_INS_NAME = "fin_ins_name";
    public static final String COLUMN_NAME_PROVIDER = "provider";
    public static final String COLUMN_NAME_CREATE_CONN_COUNT = "create_conn_count";
    public static final String COLUMN_NAME_UPDATE_CONN_COUNT = "update_conn_count";
    public static final String COLUMN_NAME_MFA_COUNT = "mfa_count";
    public static final String COLUMN_NAME_CONNECTION_STATE = "final_connection_state";
    public static final String COLUMN_NAME_ERROR_CODE = "final_error_code";
    public static final String COLUMN_NAME_ERROR_DESC = "final_error_desc";
    public static final String COLUMN_NAME_HTTP_STATUS_CODE = "final_http_status_code";
    public static final String COLUMN_NAME_CREATED_AT = "created_at";
    public static final String COLUMN_NAME_UPDATED_AT = "updated_at";

    private int mId;
    private long mUserId;
    private long mFinancialInsId;
    private String mFinancialInsName;
    private int mProvider;
    private int mCreateConnCount;
    private int mUpdateConnCount;
    private int mMfaCount;
    private String mFinalConnectionState;
    private String mFinalErrorCode;
    private String mFinalErrorDescription;
    private long mFinalHttpStatusCode;
    private Timestamp mCreatedAt;
    private Timestamp mUpdatedAt;

    public int getId() {
        return mId;
    }

    public void setId(int mId) {
        this.mId = mId;
    }

    public long getUserId() {
        return mUserId;
    }

    public void setUserId(long mUserId) {
        this.mUserId = mUserId;
    }

    public long getFinancialInsId() {
        return mFinancialInsId;
    }

    public void setFinancialInsId(long mFinancialInsId) {
        this.mFinancialInsId = mFinancialInsId;
    }

    public String getFinancialInsName() {
        return mFinancialInsName;
    }

    public void setFinancialInsName(String mFinancialInsName) {
        this.mFinancialInsName = mFinancialInsName;
    }

    public int getProvider() {
        return mProvider;
    }

    public void setProvider(int mProvider) {
        this.mProvider = mProvider;
    }

    public int getCreateConnCount() {
        return mCreateConnCount;
    }

    public void setCreateConnCount(int mCreateConnCount) {
        this.mCreateConnCount = mCreateConnCount;
    }

    public int getUpdateConnCount() {
        return mUpdateConnCount;
    }

    public void setUpdateConnCount(int mUpdateConnCount) {
        this.mUpdateConnCount = mUpdateConnCount;
    }

    public int getMfaCount() {
        return mMfaCount;
    }

    public void setMfaCount(int mMfaCount) {
        this.mMfaCount = mMfaCount;
    }

    public String getConnectionState() {
        return mFinalConnectionState;
    }

    public void setConnectionState(String mConnectionState) {
        this.mFinalConnectionState = mConnectionState;
    }

    public String getErrorCode() {
        return mFinalErrorCode;
    }

    public void setErrorCode(String mErrorCode) {
        this.mFinalErrorCode = mErrorCode;
    }

    public String getErrorDescription() {
        return mFinalErrorDescription;
    }

    public void setErrorDescription(String mErrorDescription) {
        this.mFinalErrorDescription = mErrorDescription;
    }

    public long getHttpStatusCode() {
        return mFinalHttpStatusCode;
    }

    public void setHttpStatusCode(long mHttpStatusCode) {
        this.mFinalHttpStatusCode = mHttpStatusCode;
    }

    public Timestamp getCreatedAt() {
        return mCreatedAt;
    }

    public void setCreatedAt(Timestamp mCreatedAt) {
        this.mCreatedAt = mCreatedAt;
    }

    public Timestamp getUpdatedAt() {
        return mUpdatedAt;
    }

    public void setUpdatedAt(Timestamp mUpdatedAt) {
        this.mUpdatedAt = mUpdatedAt;
    }

    public PreparedStatement getPreparedStatementForInsert(Connection connection) {
        String insertStmtStr =
                "INSERT INTO "+AURORA_BANK_CONNECTION_ANALYSIS_TABLE_NAME + "("
                        +COLUMN_NAME_USER_ID+","
                        +COLUMN_NAME_FINANCIAL_INS_ID+","+COLUMN_NAME_FINANCIAL_INS_NAME+","
                        +COLUMN_NAME_PROVIDER+","+COLUMN_NAME_CREATE_CONN_COUNT+","
                        +COLUMN_NAME_UPDATE_CONN_COUNT+","+COLUMN_NAME_MFA_COUNT+","
                        +COLUMN_NAME_CONNECTION_STATE+","+COLUMN_NAME_ERROR_CODE+","
                        +COLUMN_NAME_ERROR_DESC+","+COLUMN_NAME_HTTP_STATUS_CODE+","
                        +COLUMN_NAME_CREATED_AT+","+COLUMN_NAME_UPDATED_AT+")"
                        +" VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)";
        try {
            PreparedStatement insertStmt = connection.prepareStatement(insertStmtStr);
            insertStmt.setLong(1,getUserId());
            insertStmt.setLong(2, getFinancialInsId());
            insertStmt.setString(3,
                    (null == getFinancialInsName())?"":getSubString(getFinancialInsName(),63));
            insertStmt.setInt(4, getProvider());
            insertStmt.setInt(5,getCreateConnCount());
            insertStmt.setInt(6,getUpdateConnCount());
            insertStmt.setInt(7,getMfaCount());
            insertStmt.setString(8,
                    (null == getConnectionState())?"":getSubString(getConnectionState(),63));
            insertStmt.setString(9,
                    (null == getErrorCode())?"":getSubString(getErrorCode(),63));
            insertStmt.setString(10,
                    (null == getErrorDescription()?"":getSubString(getErrorDescription(), 127)));
            insertStmt.setLong(11,getHttpStatusCode());
            insertStmt.setTimestamp(12,new Timestamp(DateTimeUtils.getCurrentDateInEST().getTime()));
            insertStmt.setTimestamp(13,new Timestamp(DateTimeUtils.getCurrentDateInEST().getTime()));
            return insertStmt;
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    public PreparedStatement getPreparedStatementForUpdate(Connection connection) {
        String updateStmtStr =
                "UPDATE "+AURORA_BANK_CONNECTION_ANALYSIS_TABLE_NAME
                        +" SET " +COLUMN_NAME_USER_ID+"=?," +COLUMN_NAME_FINANCIAL_INS_ID+"=?,"
                        +COLUMN_NAME_FINANCIAL_INS_NAME+"=?,"+COLUMN_NAME_PROVIDER+"=?,"
                        +COLUMN_NAME_CONNECTION_STATE+"=?,"+COLUMN_NAME_CREATE_CONN_COUNT+"=?,"
                        +COLUMN_NAME_UPDATE_CONN_COUNT+"=?,"+COLUMN_NAME_MFA_COUNT+"=?,"
                        +COLUMN_NAME_ERROR_CODE+"=?,"+COLUMN_NAME_ERROR_DESC+"=?,"
                        +COLUMN_NAME_HTTP_STATUS_CODE+"=?,"+COLUMN_NAME_CREATED_AT+"=?,"
                        +COLUMN_NAME_UPDATED_AT+"=?"+" WHERE "+COLUMN_NAME_ID+"=?";

        try {
            PreparedStatement updateStmt = connection.prepareStatement(updateStmtStr);
            updateStmt.setLong(1,getUserId());
            updateStmt.setLong(2,getFinancialInsId());
            updateStmt.setString(3,
                    (null == getFinancialInsName())?"":getSubString(getFinancialInsName(),63));
            updateStmt.setInt(4, getProvider());
            updateStmt.setString(5,
                    (null == getConnectionState())?"":getSubString(getConnectionState(), 63));
            updateStmt.setInt(6,getCreateConnCount());
            updateStmt.setInt(7,getUpdateConnCount());
            updateStmt.setInt(8,getMfaCount());
            updateStmt.setString(9,
                    (null == getErrorCode())?"":getSubString(getErrorCode(), 63));
            updateStmt.setString(10,
                    (null == getErrorDescription()?"":getSubString(getErrorDescription(), 127)));
            updateStmt.setLong(11,getHttpStatusCode());
            updateStmt.setTimestamp(12,getCreatedAt());
            updateStmt.setTimestamp(13,new Timestamp(DateTimeUtils.getCurrentDateInEST().getTime()));
            updateStmt.setInt(14,getId());
            return updateStmt;
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    public PreparedStatement GetPreparedStatementForQuery(Connection connection) {
        String queryStr =
                "SELECT * FROM "+AURORA_BANK_CONNECTION_ANALYSIS_TABLE_NAME+" WHERE "
                        +COLUMN_NAME_USER_ID+"=? AND "
                        +COLUMN_NAME_PROVIDER+"=? AND "
                        +COLUMN_NAME_FINANCIAL_INS_ID+"=?";
        try {
            PreparedStatement queryStmt = connection.prepareStatement(queryStr);
            queryStmt.setLong(1, getUserId());
            queryStmt.setInt(2, getProvider());
            queryStmt.setLong(3, getFinancialInsId());
            return queryStmt;
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    private String getSubString(String str, int len) {
        if (str.length() > len) {
            return str.substring(0, len);
        }
        return str;
    }
}
