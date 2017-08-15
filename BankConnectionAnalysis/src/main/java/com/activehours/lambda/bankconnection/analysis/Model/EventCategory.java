package com.activehours.lambda.bankconnection.analysis.Model;

public enum EventCategory {
    BankConnectionAnalysis(1),
    PayrollAggregationAnalysis(2);

    private int val;
    EventCategory(int value) {
        this.val = value;
    }

    public int getVal() {
        return this.val;
    }
}
