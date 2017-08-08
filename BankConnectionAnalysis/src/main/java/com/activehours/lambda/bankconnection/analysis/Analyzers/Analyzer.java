package com.activehours.lambda.bankconnection.analysis.Analyzers;

import com.activehours.lambda.bankconnection.analysis.Model.Incoming.IncomingBankConnectionEvent;
import com.amazonaws.services.lambda.runtime.LambdaLogger;

import java.sql.Connection;
import java.util.List;

public interface Analyzer {
    void AnalyzeEvents(
            List<IncomingBankConnectionEvent> events, Connection connection, LambdaLogger logger);
}
