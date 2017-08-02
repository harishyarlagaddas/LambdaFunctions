package com.activehours.lambda.bankconnection.analysis;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import com.activehours.lambda.bankconnection.analysis.Model.BankConnectionEvent;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import jdk.nashorn.internal.parser.JSONParser;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.amazonaws.services.lambda.runtime.Context;

/**
 * A simple test harness for locally invoking your Lambda function handler.
 */
public class HourlyBankConnectionAnalysisTest {

    private static Object input;

    @BeforeClass
    public static void createInput() throws IOException {
        // TODO: set up your sample input object here.
        input = null;
    }

    private Context createContext() {
        TestContext ctx = new TestContext();

        // TODO: customize your context here if needed.
        ctx.setFunctionName("Your Function Name");

        return ctx;
    }

    //@Test
    public void testHourlyBankConnectionAnalysis() {
        HourlyBankConnectionAnalysis handler = new HourlyBankConnectionAnalysis();
        Context ctx = createContext();

        String output = handler.handleRequest(input, ctx);

        // TODO: validate output here if needed.
        Assert.assertEquals("Hello from Lambda!", output);
    }

    //@Test
    public void testErrorCodeAnalysis() {
        try {
            byte[] encoded = Files.readAllBytes(Paths.get("C:\\Users\\haris\\Downloads\\test_input.txt"));
            String input = new String(encoded);

            ObjectMapper mapper = new ObjectMapper();
            BankConnectionEvent[] events = mapper.readValue(input, BankConnectionEvent[].class);
            ArrayList<BankConnectionEvent> eventList = new ArrayList<BankConnectionEvent>(Arrays.asList(events));

            HourlyBankConnectionAnalysis analysis = new HourlyBankConnectionAnalysis();

            //Get connections stats for each of the financial institution id
            List<Integer> financialInsIds = eventList.stream().map(
                    BankConnectionEvent::getFinancialInstitutionId).distinct().collect(Collectors.toList());

            List<String> errorCodes = eventList.stream().map(
                    BankConnectionEvent::getErrorCode).distinct().collect(Collectors.toList());
            for(String errorCode: errorCodes) {
                if (null == errorCode) {
                    continue;
                }
                // Overall error codes..
                analysis.analyzeErrorCodes(eventList, errorCode, -1, new LambdaLogger() {
                    @Override
                    public void log(String s) {

                    }
                });

                for(int financialInsId: financialInsIds) {
                    analysis.analyzeErrorCodes(eventList, errorCode, financialInsId, new LambdaLogger() {
                        @Override
                        public void log(String s) {

                        }
                    });
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
