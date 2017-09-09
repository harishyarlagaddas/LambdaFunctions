package com.activehours.lambda.bankconnection.analysis;

import com.activehours.lambda.bankconnection.analysis.Analyzers.UserEventsAnalyzer;
import com.activehours.lambda.bankconnection.analysis.Model.BankConnectionEvent;
import com.activehours.lambda.bankconnection.analysis.Model.firehose.FirehoseRecord;
import com.activehours.lambda.bankconnection.analysis.Model.firehose.KinesisFirehoseEvent;
import com.activehours.lambda.bankconnection.analysis.Model.firehose.KinesisFirehoseResponse;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.DeserializationConfig;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class BankConnectionAnalysis implements RequestHandler<KinesisFirehoseEvent, KinesisFirehoseResponse> {
    private static final String PROCESS_RESULT_OK = "Ok";
    private static final String PROCESS_RESULT_DROPPED = "Dropped";
    private static final String PROCESS_RESULT_FAILED = "ProcessingFailed";

    private LambdaLogger mLogger;
    @Override
    public KinesisFirehoseResponse handleRequest(KinesisFirehoseEvent event, Context context) {
        mLogger = context.getLogger();
        KinesisFirehoseResponse response = new KinesisFirehoseResponse();
        try {
            List<BankConnectionEvent> eventList = new ArrayList<>();
            List<FirehoseRecord> processedRecords = new ArrayList<>();
            for (FirehoseRecord record : event.records) {
                String payload = new String(record.DecodeData());
                context.getLogger().log("Payload: " + payload);
                ObjectMapper mapper = new ObjectMapper();
                mapper.configure(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES, false);
                BankConnectionEvent connectionEvent = mapper.readValue(payload, BankConnectionEvent.class);
                if (1 == connectionEvent.event_type) {
                    eventList.add(connectionEvent);
                }
                record.result = PROCESS_RESULT_OK;
                processedRecords.add(record);
            }
            response.records = processedRecords;
            mLogger.log("Total Connection Events: "+eventList.size());

            UserEventsAnalyzer eventsAnalyzer = new UserEventsAnalyzer(mLogger);
            eventsAnalyzer.AnalyzeEvents(eventList);
        }catch (IOException ex) {
            mLogger.log("Exception while processing the events. Exception: "+ex.getMessage());
        }
        return response;
    }
}
