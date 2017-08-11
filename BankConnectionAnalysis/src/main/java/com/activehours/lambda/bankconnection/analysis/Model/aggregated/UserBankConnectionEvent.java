package com.activehours.lambda.bankconnection.analysis.Model.aggregated;

import org.codehaus.jackson.annotate.JsonProperty;

public class UserBankConnectionEvent {

    @JsonProperty("user_id")
    public long UserId;

    @JsonProperty("num_create_connections")
    public int NumberOfCreateConnections;

    @JsonProperty("num_update_connections")
    public int NumberOfUpdateConnections;

    @JsonProperty("num_mfa")
    public int NumberOfMfas;

    @JsonProperty("connection_state")
    public String ConnectionState;

    @JsonProperty("provider")
    public int Provider;

    @JsonProperty("error_code")
    public String ErrorCode;

    @JsonProperty("error_desc")
    public String ErrorDescription;

    @JsonProperty("fin_ins_id")
    public int FinancialInsId;

    @JsonProperty("fin_ins_name")
    public String FinancialInsName;
}
