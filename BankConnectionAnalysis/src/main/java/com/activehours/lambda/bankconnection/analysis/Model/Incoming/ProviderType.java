package com.activehours.lambda.bankconnection.analysis.Model.Incoming;

public enum ProviderType {
    All(0),
    Plaid(2),
    Mx(3),
    Chime(5);

    private int val;
    ProviderType(int value) {
        this.val = value;
    }

    public int getVal() {
        return this.val;
    }
}
