package com.activehours.lambda.bankconnection.analysis.Model;

import org.codehaus.jackson.annotate.JsonProperty;

public class BankConnectionAnalysisEvent {

	@JsonProperty("provider_type")
	public int ProviderType;

	@JsonProperty("financial_ins_id")
	public int FinancialInstitutionId;

	@JsonProperty("total_create_connections")
	public long TotalCreateConnections;

	@JsonProperty("number_of_successful_create_connections")
	public long NumberOfSuccessfulCreateConnections;

	@JsonProperty("number_of_failed_create_connections")
	public long NumberOfFailedCreateConnections;

	@JsonProperty("number_of_mfas_after_create_connection")
	public long NumberOfMfasAfterCreateConnection;

	@JsonProperty("number_of_non_deterministic_after_create_connection")
	public long NumberOfNoDeterministicStateAfterCreateConnection;

	@JsonProperty("total_mfa_connections")
	public long TotalMfaConnections;

	@JsonProperty("number_of_successful_mfa_connections")
	public long NumberOfSuccessfulMfaConnections;

	@JsonProperty("number_of_failed_mfa_connections")
	public long NumberOfFailedMfaConnections;

	@JsonProperty("number_of_mfas_after_submitting_mfa")
	public long NumberOfMfasAfterSubmittingMfa;

	@JsonProperty("number_of_non_deterministic_after_submitting_mfa")
	public long NumberOfNoDeterministicStateAfterSubmittingMfa;

	@JsonProperty("total_update_connections")
	public long TotalUpdateConnections;

	@JsonProperty("number_of_successful_update_connections")
	public long NumberOfSuccessfulUpdateConnections;

	@JsonProperty("number_of_failed_update_connections")
	public long NumberOfFailedUpdateConnections;

	@JsonProperty("number_of_mfas_after_update_connection")
	public long NumberOfMfasAfterUpdateConnection;

	@JsonProperty("number_of_non_deterministic_after_update_connection")
	public long NumberOfNoDeterministicStateAfterUpdateConnection;


	public void setTotalConnections(ConnectionType connectionType, long totalConnections) {
		switch (connectionType) {
			case Create:
				this.TotalCreateConnections = totalConnections;
				break;
			case Update:
				this.TotalUpdateConnections = totalConnections;
				break;
			case Mfa:
				this.TotalMfaConnections = totalConnections;
				break;
		}
	}

	public void setSuccessfulConnections(ConnectionType connectionType, long connections) {
		switch (connectionType) {
			case Create:
				this.NumberOfSuccessfulCreateConnections = connections;
				break;
			case Update:
				this.NumberOfSuccessfulUpdateConnections = connections;
				break;
			case Mfa:
				this.NumberOfSuccessfulMfaConnections = connections;
				break;
		}
	}

	public void setFailureConnections(ConnectionType connectionType, long connections) {
		switch (connectionType) {
			case Create:
				this.NumberOfFailedCreateConnections = connections;
				break;
			case Update:
				this.NumberOfFailedUpdateConnections = connections;
				break;
			case Mfa:
				this.NumberOfFailedMfaConnections = connections;
				break;
		}
	}

	public void setNonDeterministicConnections(ConnectionType connectionType, long connections) {
		switch (connectionType) {
			case Create:
				this.NumberOfNoDeterministicStateAfterCreateConnection = connections;
				break;
			case Update:
				this.NumberOfNoDeterministicStateAfterUpdateConnection = connections;
				break;
			case Mfa:
				this.NumberOfNoDeterministicStateAfterSubmittingMfa = connections;
				break;
		}
	}

	public void setMfaConnections(ConnectionType connectionType, long connections) {
		switch (connectionType) {
			case Create:
				this.NumberOfMfasAfterCreateConnection = connections;
				break;
			case Update:
				this.NumberOfMfasAfterUpdateConnection = connections;
				break;
			case Mfa:
				this.NumberOfMfasAfterSubmittingMfa = connections;
				break;
		}
	}
}

