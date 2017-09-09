package com.activehours.lambda.bankconnection.analysis.Analyzers;


import com.activehours.lambda.bankconnection.analysis.Model.BankConnectionEvent;
import com.activehours.lambda.bankconnection.analysis.Model.enums.ConnectionStatus;
import com.activehours.lambda.bankconnection.analysis.Model.enums.ConnectionType;
import com.activehours.lambda.bankconnection.analysis.Model.aurora.BankConnectionAnalysisEvent;
import com.activehours.lambda.bankconnection.analysis.database.AuroraDatabase;
import com.amazonaws.services.lambda.runtime.LambdaLogger;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

public class UserEventsAnalyzer {
    private AuroraDatabase mDatabase;
    private LambdaLogger mLogger;

    public UserEventsAnalyzer(LambdaLogger logger) {
        mLogger = logger;
        mDatabase = new AuroraDatabase(logger);
    }

    public void AnalyzeEvents(
            List<BankConnectionEvent> events) {

        if (0 == events.size()) {
            mLogger.log("UserEventsAnalyzer incoming events are of zero size. Hence can not analyze the events");
            return;
        }

        List<Long> users = events.stream().map(
                BankConnectionEvent::getUserId).distinct().collect(Collectors.toList());

        mLogger.log("Got number of distinct Users: "+users.size());
        for(Long user: users) {
            mLogger.log("Processing events for User: "+user);
            List<BankConnectionEvent> userEvents = events.stream()
                    .filter(event -> event.UserId == user).collect(Collectors.toList());

            List<Long> financialInsIds = userEvents.stream().map(
                    BankConnectionEvent::getFinancialInstitutionId).distinct().collect(Collectors.toList());
            mLogger.log("Got number of distinct Financial Instituion Ids: "+financialInsIds.size());
            for (long finInsID: financialInsIds) {
                mLogger.log("Processing events for Financial Institution ID: "+finInsID);
                List<BankConnectionEvent> finInsEvents = userEvents.stream()
                        .filter(event -> event.FinancialInstitutionId == finInsID).collect(Collectors.toList());

                List<Integer> providers = finInsEvents.stream().map(
                        BankConnectionEvent::getBankFeedProvider).distinct().collect(Collectors.toList());
                mLogger.log("Got number of distinct providers: "+providers.size());
                for (int provider: providers) {
                    mLogger.log("Processing events for Provider: "+provider);
                    List<BankConnectionEvent> providerList = finInsEvents.stream()
                            .filter(event -> event.BankFeedProvider == provider).collect(Collectors.toList());
                    processUserEvents(providerList);
                }
            }
        }
    }

    private void processUserEvents(List<BankConnectionEvent> userEvents) {
        if (0 == userEvents.size()) {
            mLogger.log("UserEventsAnalyzer:ProcessUserEvents incoming userEvents is of zero size");
        }

        //First sort the list based on eventTIme
        Collections.sort(userEvents, Comparator.comparing(o -> o.CreatedAt));

        BankConnectionAnalysisEvent analysisEvent;

        // Since for all these events userId, provider and financialInsId are same get from 1st event.
        BankConnectionEvent firstEvent = userEvents.get(0);
        mLogger.log("Processing the userEvents for userID: "+firstEvent.getUserId()
                +"Financial Instituion ID: "+firstEvent.getFinancialInstitutionId()
                +"Provider: "+firstEvent.getBankFeedProvider());
        boolean createEvent = false;
        List<BankConnectionAnalysisEvent> eventList = mDatabase.ReadAnalysisEvents(
                firstEvent.UserId, firstEvent.FinancialInstitutionId, firstEvent.BankFeedProvider);

        if (eventList.size() > 0) {
            analysisEvent = eventList.get(eventList.size()-1);
            mLogger.log("Got the analysisEvent from DB. userID: "+analysisEvent.getUserId()
                    +" Financial Instituion ID: "+analysisEvent.getFinancialInsId()
                    +"Provider: "+analysisEvent.getProvider());
        } else {
            mLogger.log("Didn't get analysisEvent from DB. Hence creating the new one.");
            createEvent = true;
            analysisEvent = new BankConnectionAnalysisEvent();
        }

        int createConnCount = (int)
                userEvents.stream().filter(event -> event.ConnectionType == ConnectionType.Create.getVal()).count();
        int updateConnCount = (int)
                userEvents.stream().filter(event -> event.ConnectionType == ConnectionType.Update.getVal()).count();
        int mfaCount = (int)
                userEvents.stream().filter(event -> event.Status == ConnectionStatus.Mfa.getVal()).count();

        analysisEvent.setUserId(firstEvent.UserId);
        analysisEvent.setFinancialInsId(firstEvent.FinancialInstitutionId);
        analysisEvent.setFinancialInsName(firstEvent.FinancialInstitutionName);
        analysisEvent.setProvider(firstEvent.BankFeedProvider);

        // Assign the final connection state, error codes and error descriptions and ignore all the previous ones
        BankConnectionEvent finalEvent = userEvents.get(userEvents.size()-1);
        analysisEvent.setConnectionState(finalEvent.ConnectionState);
        analysisEvent.setErrorCode(finalEvent.ErrorCode);
        analysisEvent.setErrorDescription(finalEvent.ErrorDescription);
        analysisEvent.setHttpStatusCode(finalEvent.HttpStatusCode);

        analysisEvent.setCreateConnCount(analysisEvent.getCreateConnCount()+createConnCount);
        analysisEvent.setUpdateConnCount(analysisEvent.getUpdateConnCount()+updateConnCount);
        analysisEvent.setMfaCount(analysisEvent.getMfaCount()+mfaCount);

        if (createEvent) {
            mDatabase.CreateAnalysisEvent(analysisEvent);
        } else {
            mDatabase.UpdateAnalysisEvent(analysisEvent);
        }
    }
}
