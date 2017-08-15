package com.activehours.lambda.bankconnection.analysis.Model;

import java.sql.Timestamp;
import java.util.Date;

public class BankConnectionEvent {

    public static final String COLUMN_NAME_ROW_NUMBER = "RowNumber";
    public static final String COLUMN_NAME_CONNECTION_TYPE = "conn_type";
    public static final String COLUMN_NAME_IS_AUTH_ENABLED = "auth_enabled";
    public static final String COLUMN_NAME_ERROR_CODE = "provider_error_code";
    public static final String COLUMN_NAME_ERROR_DESCRIPTION = "provider_error_description";
    public static final String COLUMN_NAME_REQUEST_TIME = "time_taken";
    public static final String COLUMN_NAME_STATUS = "status";
    public static final String COLUMN_NAME_USERID = "userid";
    public static final String COLUMN_NAME_FINANCIAL_INSTITUTION_ID = "fin_ins_id";
    public static final String COLUMN_NAME_FINANCIAL_INSTITUTION_NAME = "fin_ins_name";
    public static final String COLUMN_NAME_BANKFEED_PROVIDER = "bankfeed_provider";
    public static final String COLUMN_NAME_HTTP_STATUS_CODE = "http_status_code";
    public static final String COLUMN_NAME_USER_PROVIDED_BANK_CONNECTION_ID = "upbc_id";
    public static final String COLUMN_NAME_BFP_PROVIDED_BANK_CONNECTION_ID = "bpbc_id";
    public static final String COLUMN_NAME_EVENT_CREATION_TIME = "created_at";
    public static final String COLUMN_NAME_CONNECTION_STATE = "conn_state";

    public int ConnectionType;
    public boolean IsAuthEnabled;
    public String ErrorCode;
    public String ErrorDescription;
    public long RequestTime;
    public int Status;
    public long UserId;
    public int FinancialInstitutionId;
    public String FinancialInstitutionName;
    public int BankFeedProvider;
    public int HttpStatusCode;
    public long UserProvidedBankConnectionId;
    public long BfpProvidedBankConnectionId;
    public String ConnectionState;
    public Timestamp EventCreationTime;

    public int getFinancialInstitutionId() {
        return FinancialInstitutionId;
    }

    public String getErrorCode() {
        return ErrorCode;
    }

    public String getConnectionState() {
        return ConnectionState;
    }

    public int getBankFeedProvider() { return BankFeedProvider;}
}
