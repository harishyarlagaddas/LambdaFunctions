package com.activehours.lambda.bankconnection.analysis.Model.enums;

public enum EventType {
    BankConnectionEvent(1);

    private int val;
    EventType(int value) {
        this.val = value;
    }

    public int getVal() {
        return this.val;
    }
}
