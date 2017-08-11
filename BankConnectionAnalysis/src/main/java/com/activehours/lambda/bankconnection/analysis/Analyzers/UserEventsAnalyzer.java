package com.activehours.lambda.bankconnection.analysis.Analyzers;


import com.activehours.lambda.bankconnection.analysis.Model.Incoming.ConnectionStatus;
import com.activehours.lambda.bankconnection.analysis.Model.Incoming.ConnectionType;
import com.activehours.lambda.bankconnection.analysis.Model.Incoming.IncomingBankConnectionEvent;
import com.activehours.lambda.bankconnection.analysis.Model.aggregated.UserBankConnectionEvent;
import com.activehours.lambda.bankconnection.analysis.S3Uploader;
import com.amazonaws.services.lambda.runtime.LambdaLogger;

import java.sql.Connection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class UserEventsAnalyzer implements Analyzer {
    private S3Uploader mS3Uploader;

    public UserEventsAnalyzer(S3Uploader s3Uploader) {
        mS3Uploader = s3Uploader;
    }

    @Override
    public void AnalyzeEvents(
            List<IncomingBankConnectionEvent> events,
            Connection connection, LambdaLogger logger) {

        if (0 == events.size()) {
            logger.log("UserEventsAnalyzer incoming events are of zero size. Hence can not analyze the events");
        }

        List<Integer> providers = events.stream().map(
                IncomingBankConnectionEvent::getBankFeedProvider).distinct().collect(Collectors.toList());

        for (int provider: providers) {
            Stream<IncomingBankConnectionEvent> providerEventStream = events.stream()
                    .filter(event -> event.BankFeedProvider == provider);
            List<IncomingBankConnectionEvent> filteredProviderList = providerEventStream.collect(Collectors.toList());

            List<Integer> financialInsIds = filteredProviderList.stream().map(
                    IncomingBankConnectionEvent::getFinancialInstitutionId).distinct().collect(Collectors.toList());

            for (int finInsId: financialInsIds) {
                Stream<IncomingBankConnectionEvent> finEventStream = filteredProviderList.stream()
                        .filter(event -> event.FinancialInstitutionId == finInsId);
                List<IncomingBankConnectionEvent> filteredFinInsList = finEventStream.collect(Collectors.toList());
                logger.log("ProcessUserEvents events size: "+filteredFinInsList.size()
                        +" with Provider: "+provider+" FinancialInsId: "+finInsId);
                processUserEvents(filteredFinInsList, logger);
            }
        }
    }

    private void processUserEvents(List<IncomingBankConnectionEvent> userEvents, LambdaLogger logger) {
        if (0 == userEvents.size()) {
            logger.log("UserEventsAnalyzer:ProcessUserEvents incoming userEvents is of zero size");
        }

        //First sort the list based on eventTIme
        Collections.sort(userEvents, Comparator.comparing(o -> o.EventCreationTime));

        UserBankConnectionEvent bankConnectionEvent = new UserBankConnectionEvent();

        // Since for all these events userId, provider and financialInsId are same get from 1st event.
        IncomingBankConnectionEvent firstEvent = userEvents.get(0);
        bankConnectionEvent.UserId = firstEvent.UserId;
        bankConnectionEvent.Provider = firstEvent.BankFeedProvider;
        bankConnectionEvent.FinancialInsId = firstEvent.FinancialInstitutionId;
        bankConnectionEvent.FinancialInsName = firstEvent.FinancialInstitutionName;
        bankConnectionEvent.NumberOfMfas = (int)
                userEvents.stream().filter(event -> event.Status == ConnectionStatus.Mfa.getVal()).count();
        bankConnectionEvent.NumberOfCreateConnections = (int)
                userEvents.stream().filter(event -> event.ConnectionType == ConnectionType.Create.getVal()).count();
        bankConnectionEvent.NumberOfUpdateConnections = (int)
                userEvents.stream().filter(event -> event.ConnectionType == ConnectionType.Update.getVal()).count();

        // Assign the final connection state, error codes and error descriptions and ignore all the previous ones
        IncomingBankConnectionEvent finalEvent = userEvents.get(userEvents.size()-1);
        bankConnectionEvent.ConnectionState = finalEvent.ConnectionState;
        bankConnectionEvent.ErrorCode = finalEvent.ErrorCode;
        bankConnectionEvent.ErrorDescription = finalEvent.ErrorDescription;
        mS3Uploader.uploadEvent(bankConnectionEvent);
    }
}
