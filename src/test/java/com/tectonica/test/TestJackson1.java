package com.tectonica.test;

import java.util.logging.Logger;

import org.codehaus.jackson.annotate.JsonPropertyOrder;
import org.codehaus.jackson.annotate.JsonSubTypes;
import org.codehaus.jackson.annotate.JsonSubTypes.Type;
import org.codehaus.jackson.annotate.JsonTypeInfo;
import org.codehaus.jackson.annotate.JsonTypeInfo.As;
import org.junit.Assert;
import org.junit.Test;

import com.tectonica.thirdparty.Jackson1;

public class TestJackson1
{
	private static Logger LOG = Logger.getLogger(TestJackson1.class.getSimpleName());

	public static enum PushType
	{
		JobOffer, Alert;
	}

	@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = As.PROPERTY, property = "type")
	@JsonSubTypes({ @Type(value = JobPN.class, name = "job"), @Type(value = AlertPN.class, name = "alert") })
	public static class PN
	{
		public String pushId = "push1";
	}

	@JsonPropertyOrder({ "type", "pushId", "jobId" })
	public static class JobPN extends PN
	{
		public String jobId = "job1";
	}

	@JsonPropertyOrder({ "type", "pushId", "alertId" })
	public static class AlertPN extends PN
	{
		public String alertId = "alert1";
	}

	@Test
	public void test()
	{
		// serialization
		String jobJson = Jackson1.fieldsToJson(new JobPN());
		String alertJson = Jackson1.fieldsToJson(new AlertPN());
		LOG.info(jobJson);
		LOG.info(alertJson);

		// de-serialization
		PN obj = Jackson1.fieldsFromJson(jobJson, PN.class);
		Assert.assertTrue(obj.getClass() == JobPN.class);
	}
}
