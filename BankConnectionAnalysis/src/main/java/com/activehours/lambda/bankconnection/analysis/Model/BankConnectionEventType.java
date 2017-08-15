package com.activehours.lambda.bankconnection.analysis.Model;

public enum BankConnectionEventType {
    ConnectionEvent (1),
    ErrorEvent(2),
    ConnectionStatusEvent(3);

    private int val;
    BankConnectionEventType(int value) {
        this.val = value;
    }

    public int getVal() {
        return val;
    }
}
