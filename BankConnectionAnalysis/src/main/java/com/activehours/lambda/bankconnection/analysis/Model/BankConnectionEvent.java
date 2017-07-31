package com.activehours.lambda.bankconnection.analysis.Model;

public class BankConnectionEvent {

    public static final String COLUMN_NAME_CONNECTION_TYPE = "ConnectionType";
    public static final String COLUMN_NAME_IS_AUTH_ENABLED = "IsAuthEnabled";
    public static final String COLUMN_NAME_ERROR_CODE = "ErrorCode";
    public static final String COLUMN_NAME_ERROR_DESCRIPTION = "ErrorDescription";
    public static final String COLUMN_NAME_REQUEST_TIME = "RequestTime";
    public static final String COLUMN_NAME_STATUS = "Status";
    public static final String COLUMN_NAME_USERID = "UserId";
    public static final String COLUMN_NAME_FINANCIAL_INSTITUTION_ID = "FinancialInstituionId";
    public static final String COLUMN_NAME_BANKFEED_PROVIDER = "BankFeedProvider";
    public static final String COLUMN_NAME_HTTP_STATUS_CODE = "HttpStatusCode";
    public static final String COLUMN_NAME_USER_PROVIDED_BANK_CONNECTION_ID = "UserProvidedBankConnectionId";
    public static final String COLUMN_NAME_BFP_PROVIDED_BANK_CONNECTION_ID = "BfpProvidedBankConnectionId";

    public int ConnectionType;
    public boolean IsAuthEnabled;
    public String ErrorCode;
    public String ErrorDescription;
    public long RequestTime;
    public int Status;
    public long UserId;
    public int FinancialInstitutionId;
    public int BankFeedProvider;
    public int HttpStatusCode;
    public long UserProvidedBankConnectionId;
    public long BfpProvidedBankConnectionId;

    public int getFinancialInstitutionId() {
        return FinancialInstitutionId;
    }

    public String getErrorCode() {
        return ErrorCode;
    }
}
