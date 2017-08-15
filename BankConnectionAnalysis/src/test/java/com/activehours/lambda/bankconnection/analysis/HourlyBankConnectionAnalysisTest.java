package com.activehours.lambda.bankconnection.analysis;

import com.activehours.lambda.bankconnection.analysis.Model.BankConnectionEvent;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.math.RoundingMode;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;

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

    @Test
    public void testErrorCodeAnalysis() {
        try {
            DecimalFormat mFormat= new DecimalFormat("00");
            mFormat.setRoundingMode(RoundingMode.DOWN);

            Calendar calInEst = Calendar.getInstance(TimeZone.getTimeZone("America/New_York"));
            calInEst.add(Calendar.HOUR_OF_DAY,-1);

            calInEst.set(Calendar.MINUTE, 00);
            calInEst.set(Calendar.SECOND, 00);
            calInEst.set(Calendar.MILLISECOND, 000);
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
            dateFormat.setTimeZone(TimeZone.getTimeZone("America/New_York"));
            String fromDateStr = dateFormat.format(calInEst.getTime());

            calInEst.set(Calendar.MINUTE, 59);
            calInEst.set(Calendar.SECOND, 59);
            calInEst.set(Calendar.MILLISECOND, 999);
            String toDateStr = dateFormat.format(calInEst.getTime());

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
