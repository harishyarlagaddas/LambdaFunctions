package com.activehours.lambda.bankconnection.analysis.Analyzers;

import com.activehours.lambda.bankconnection.analysis.Model.BankConnectionEvent;
import com.amazonaws.services.lambda.runtime.LambdaLogger;

import java.sql.Connection;
import java.util.List;

public interface EventsAnalyzer {
    void AnalyzeEvents(
            List<BankConnectionEvent> events, Connection connection, LambdaLogger logger);
}
