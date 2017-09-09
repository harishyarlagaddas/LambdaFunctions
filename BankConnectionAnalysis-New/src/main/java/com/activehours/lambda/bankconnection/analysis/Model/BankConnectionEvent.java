package com.activehours.lambda.bankconnection.analysis.Model;

import com.activehours.lambda.bankconnection.analysis.utils.CustomDateDeserializer;
import org.codehaus.jackson.annotate.JsonProperty;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.codehaus.jackson.map.annotate.JsonDeserialize;

import java.sql.Timestamp;
import java.util.Date;

@JsonIgnoreProperties(ignoreUnknown = true)
public class BankConnectionEvent {

    @JsonProperty("status")
    public int Status;

    @JsonProperty("conn_type")
    public int ConnectionType;

    @JsonProperty("auth_enabled")
    public boolean IsAuthEnabled;

    @JsonProperty("provider_error_code")
    public String ErrorCode;

    @JsonProperty("provider_error_description")
    public String ErrorDescription;

    @JsonProperty("time_taken")
    public long RequestTime;

    @JsonProperty("conn_state")
    public String ConnectionState;

    @JsonProperty("fin_ins_id")
    public long FinancialInstitutionId;

    @JsonProperty("fin_ins_name")
    public String FinancialInstitutionName;

    @JsonProperty("bankfeed_provider")
    public int BankFeedProvider;

    @JsonProperty("http_status_code")
    public long HttpStatusCode;

    @JsonProperty("upbc_id")
    public long UserProvidedBankConnectionId;

    @JsonProperty("bpbc_id")
    public long BfpProvidedBankConnectionId;

    @JsonProperty("userid")
    public long UserId;

    @JsonProperty("event_category")
    public int EventCategory;

    @JsonProperty("event_type")
    public int event_type;

    @JsonProperty("created_at")
    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss")
    @JsonDeserialize(using=CustomDateDeserializer.class)
    public Date CreatedAt;

    @JsonProperty("context_id")
    public String ContextId;

    public long getFinancialInstitutionId() {
        return FinancialInstitutionId;
    }

    public String getErrorCode() {
        return ErrorCode;
    }

    public String getConnectionState() {
        return ConnectionState;
    }

    public int getBankFeedProvider() { return BankFeedProvider;}

    public long getUserId() { return UserId;}
}
