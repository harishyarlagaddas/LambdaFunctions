package com.activehours.lambda.bankconnection.analysis.Model.firehose;

import java.util.Base64;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

public class KinesisFirehoseEvent {
	
	public String invocationId;

    public String deliveryStreamArn;

    public String region;

    public List<FirehoseRecord> records;

}
