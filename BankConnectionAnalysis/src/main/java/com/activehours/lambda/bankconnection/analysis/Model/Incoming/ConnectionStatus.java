package com.activehours.lambda.bankconnection.analysis.Model.Incoming;

public enum ConnectionStatus {
    Success (0),
    Mfa (1),
    PendingDeterministicStatus (2),
    Failure (-1);

    private int val;
    ConnectionStatus(int value) {
        this.val = value;
    }

    public int getVal() {
        return this.val;
    }
}
