package com.activehours.lambda.bankconnection.analysis.Model.Analyzed;

import org.codehaus.jackson.annotate.JsonProperty;

public class BankConnectionErrorEvent {

    @JsonProperty("event_type")
    public int EventType;

    @JsonProperty("provider_type")
    public int ProviderType;

    @JsonProperty("financial_ins_id")
    public int FinancialInstitutionId;

    @JsonProperty("error_code")
    public String ErrorCode;

    @JsonProperty("error_count")
    public long ErrorCount;

    @JsonProperty("total_create_connections")
    public long TotalNumberOfCreateConnections;

    @JsonProperty("total_update_connections")
    public long TotalNumberOfUpdateConnections;

    @JsonProperty("total_mfa_connections")
    public long TotalNumberOfSubmittingMfaAnswers;

    public enum ErrorType{
        ProviderError,
        ConnectionState
    }
}