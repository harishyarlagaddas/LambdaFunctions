package com.activehours.lambda.bankconnection.analysis.Model.Incoming;

public enum ConnectionType {
    Create(1),
    Update(2),
    Mfa(3);

    private int val;
    ConnectionType(int value) {
        this.val = value;
    }

    public int getVal() {
        return this.val;
    }
}
