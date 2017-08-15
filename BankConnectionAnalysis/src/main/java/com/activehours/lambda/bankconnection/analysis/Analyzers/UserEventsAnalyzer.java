package com.activehours.lambda.bankconnection.analysis.Analyzers;


import com.activehours.lambda.bankconnection.analysis.Model.BankConnectionEvent;
import com.activehours.lambda.bankconnection.analysis.Model.ConnectionStatus;
import com.activehours.lambda.bankconnection.analysis.Model.ConnectionType;
import com.activehours.lambda.bankconnection.analysis.Model.AggregatedBankConnectionEvent;
import com.activehours.lambda.bankconnection.analysis.S3Uploader;
import com.amazonaws.services.lambda.runtime.LambdaLogger;

import java.sql.Connection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class UserEventsAnalyzer implements EventsAnalyzer {
    private S3Uploader mS3Uploader;

    public UserEventsAnalyzer(S3Uploader s3Uploader) {
        mS3Uploader = s3Uploader;
    }

    @Override
    public void AnalyzeEvents(
            List<BankConnectionEvent> events,
            Connection connection, LambdaLogger logger) {

        if (0 == events.size()) {
            logger.log("UserEventsAnalyzer incoming events are of zero size. Hence can not analyze the events");
        }

        List<Integer> providers = events.stream().map(
                BankConnectionEvent::getBankFeedProvider).distinct().collect(Collectors.toList());

        for (int provider: providers) {
            Stream<BankConnectionEvent> providerEventStream = events.stream()
                    .filter(event -> event.BankFeedProvider == provider);
            List<BankConnectionEvent> filteredProviderList = providerEventStream.collect(Collectors.toList());

            List<Integer> financialInsIds = filteredProviderList.stream().map(
                    BankConnectionEvent::getFinancialInstitutionId).distinct().collect(Collectors.toList());

            for (int finInsId: financialInsIds) {
                Stream<BankConnectionEvent> finEventStream = filteredProviderList.stream()
                        .filter(event -> event.FinancialInstitutionId == finInsId);
                List<BankConnectionEvent> filteredFinInsList = finEventStream.collect(Collectors.toList());
                processUserEvents(filteredFinInsList, logger);
            }
        }
    }

    private void processUserEvents(List<BankConnectionEvent> userEvents, LambdaLogger logger) {
        if (0 == userEvents.size()) {
            logger.log("UserEventsAnalyzer:ProcessUserEvents incoming userEvents is of zero size");
        }

        //First sort the list based on eventTIme
        Collections.sort(userEvents, Comparator.comparing(o -> o.EventCreationTime));

        AggregatedBankConnectionEvent bankConnectionEvent = new AggregatedBankConnectionEvent();

        // Since for all these events userId, provider and financialInsId are same get from 1st event.
        BankConnectionEvent firstEvent = userEvents.get(0);
        bankConnectionEvent.UserId = firstEvent.UserId;
        bankConnectionEvent.Provider = firstEvent.BankFeedProvider;
        bankConnectionEvent.FinancialInsId = firstEvent.FinancialInstitutionId;
        bankConnectionEvent.FinancialInsName = firstEvent.FinancialInstitutionName;
        bankConnectionEvent.CreatedAt = firstEvent.EventCreationTime;

        bankConnectionEvent.NumberOfMfas = (int)
                userEvents.stream().filter(event -> event.Status == ConnectionStatus.Mfa.getVal()).count();
        bankConnectionEvent.NumberOfCreateConnections = (int)
                userEvents.stream().filter(event -> event.ConnectionType == ConnectionType.Create.getVal()).count();
        bankConnectionEvent.NumberOfUpdateConnections = (int)
                userEvents.stream().filter(event -> event.ConnectionType == ConnectionType.Update.getVal()).count();

        // Assign the final connection state, error codes and error descriptions and ignore all the previous ones
        BankConnectionEvent finalEvent = userEvents.get(userEvents.size()-1);
        bankConnectionEvent.ConnectionState = finalEvent.ConnectionState;
        bankConnectionEvent.ErrorCode = finalEvent.ErrorCode;
        bankConnectionEvent.ErrorDescription = finalEvent.ErrorDescription;
        mS3Uploader.uploadEvent(bankConnectionEvent);
    }
}
