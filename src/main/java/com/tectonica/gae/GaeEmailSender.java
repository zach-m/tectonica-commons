/*
 * Copyright (C) 2014 Zach Melamed
 * 
 * Latest version available online at https://github.com/zach-m/tectonica-commons
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.tectonica.gae;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;

import javax.activation.DataHandler;
import javax.mail.BodyPart;
import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.AddressException;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeBodyPart;
import javax.mail.internet.MimeMessage;
import javax.mail.internet.MimeMultipart;
import javax.mail.util.ByteArrayDataSource;

/**
 * Utility class for sending emails using GAE JavaMail implementation (which is different - in several aspects - from Oracle's) without the
 * hassle of manually assembling Multipart data. Implemented using the Builder pattern for easy invocation.
 * <p>
 * Examples:
 * 
 * <pre>
 * // send an email with TEXT-only body
 * GaeEmailSender.subject(&quot;Hello&quot;).to(&quot;a@y.com&quot;).cc(&quot;b@y.com&quot;).text(&quot;body as text&quot;).send();
 * 
 * // send an email with HTML + TEXT version of the body
 * GaeEmailSender.subject(&quot;Hello&quot;).to(&quot;x@y.com&quot;).text(&quot;body as text&quot;).html(&quot;&lt;html&gt;&lt;body&gt;body as html&lt;/body&gt;&lt;/html&gt;&quot;).send();
 * 
 * // send an email with HTML body + two attachments
 * GaeEmailSender.subject(&quot;..&quot;).to(&quot;..&quot;).html(&quot;..&quot;).attach(&quot;my.pdf&quot;, &quot;application/pdf&quot;, pdfBytes).attach(&quot;me.png&quot;, &quot;image/png&quot;, pngBytes)
 * 		.send();
 * </pre>
 * 
 * @author Zach Melamed
 */
public class GaeEmailSender
{
	/**
	 * this email-address must be listed in the GAE application Permissions area
	 */
//	private static final String DEFAULT_FROM = "Example <" + System.getProperties().get("com.google.appengine.application.id")
//			+ "@appspot.gserviceaccount.com>";
	private static String defaultFrom = null; // "Example <jack@example.com>";

	private static String defaultReplyTo = null; // "noreply@example.com";

	public static void setDefaultFrom(String defaultFrom)
	{
		GaeEmailSender.defaultFrom = defaultFrom;
	}

	public static void setDefaultReplyTo(String defaultReplyTo)
	{
		GaeEmailSender.defaultReplyTo = defaultReplyTo;
	}

	private static class Attachment
	{
		String filename;
		String mimeType;
		byte[] data;

		Attachment(String filename, String mimeType, byte[] data)
		{
			this.filename = filename;
			this.mimeType = mimeType;
			this.data = data;
		}
	}

	private MimeMessage msg = new MimeMessage(getGaeSession());;
	private String mText;
	private String mHtml;
	private List<Attachment> mAttachments = new ArrayList<>();

	public static GaeEmailSender subject(String subject) throws MessagingException
	{
		GaeEmailSender sender = new GaeEmailSender();
		sender.msg.setSubject(subject, "UTF-8");
		if (defaultFrom != null)
			sender.from(defaultFrom);
		if (defaultReplyTo != null)
			sender.replyTo(defaultReplyTo);
		return sender;
	}

	public GaeEmailSender from(String from) throws AddressException, MessagingException
	{
		msg.setFrom(new InternetAddress(from));
		return this;
	}

	public GaeEmailSender replyTo(String replyTo) throws AddressException, MessagingException
	{
		msg.setReplyTo(InternetAddress.parse(replyTo.replaceAll(";", ",")));
		return this;
	}

	public GaeEmailSender to(String to) throws AddressException, MessagingException
	{
		msg.addRecipients(Message.RecipientType.TO, InternetAddress.parse(to.replaceAll(";", ",")));
		return this;
	}

	public GaeEmailSender cc(String cc) throws AddressException, MessagingException
	{
		msg.addRecipients(Message.RecipientType.CC, InternetAddress.parse(cc.replaceAll(";", ",")));
		return this;
	}

	public GaeEmailSender bcc(String bcc) throws AddressException, MessagingException
	{
		msg.addRecipients(Message.RecipientType.BCC, InternetAddress.parse(bcc.replaceAll(";", ",")));
		return this;
	}

	public GaeEmailSender text(String text) throws MessagingException
	{
		mText = text;
		return this;
	}

	public GaeEmailSender html(String html) throws MessagingException
	{
		mHtml = html;
		return this;
	}

	public GaeEmailSender attach(String filename, String mimeType, byte[] data) throws MessagingException
	{
		mAttachments.add(new Attachment(filename, mimeType, data));
		return this;
	}

	public void send() throws MessagingException
	{
		if (mText == null && mHtml == null)
			throw new NullPointerException("At least one context has to be provided: Text or Html");

		MimeMultipart content = new MimeMultipart();
		content.addBodyPart(textPart()); // plain text is always looked for by GAE's JavaMail
		if (mHtml != null)
			content.addBodyPart(htmlPart());

		for (Attachment attachment : mAttachments)
		{
			BodyPart attachPart = new MimeBodyPart();
			attachPart.setFileName(attachment.filename);
			attachPart.setDataHandler(new DataHandler(new ByteArrayDataSource(attachment.data, attachment.mimeType)));
			attachPart.setDisposition("attachment");
			content.addBodyPart(attachPart);
		}

		msg.setContent(content);
		msg.setSentDate(new Date());

		Transport.send(msg);
	}

	private MimeBodyPart textPart() throws MessagingException
	{
		MimeBodyPart text = new MimeBodyPart();
		text.setText(mText == null ? " " : mText);
		return text;
	}

	private MimeBodyPart htmlPart() throws MessagingException
	{
		MimeBodyPart html = new MimeBodyPart();
		html.setContent(mHtml, "text/html; charset=utf-8");
		return html;
	}

	/**
	 * returns an "empty" session, filled automatically by GAE
	 */
	private static Session getGaeSession()
	{
		Properties props = new Properties();
		return Session.getDefaultInstance(props, null);
	}
}
