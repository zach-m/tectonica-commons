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

package com.tectonica.util;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;

import javax.activation.DataHandler;
import javax.mail.Authenticator;
import javax.mail.BodyPart;
import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.PasswordAuthentication;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.AddressException;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeBodyPart;
import javax.mail.internet.MimeMessage;
import javax.mail.internet.MimeMultipart;
import javax.mail.util.ByteArrayDataSource;

/**
 * Utility class to use JavaMail without the hassle of manually assembling Multipart data. Implemented using the Builder pattern for
 * easy invocation.
 * <p>
 * Examples:
 * 
 * <pre>
 * // send an email with TEXT-only body
 * EmailSender.subject(&quot;Hello&quot;).to(&quot;a@y.com&quot;).cc(&quot;b@y.com&quot;).text(&quot;body as text&quot;).send();
 * 
 * // send an email with HTML + TEXT version of the body
 * EmailSender.subject(&quot;Hello&quot;).to(&quot;x@y.com&quot;).text(&quot;body as text&quot;).html(&quot;&lt;html&gt;&lt;body&gt;body as html&lt;/body&gt;&lt;/html&gt;&quot;).send();
 * 
 * // send an email with HTML body + two attachments
 * EmailSender.subject(&quot;..&quot;).to(&quot;..&quot;).html(&quot;..&quot;).attach(&quot;my.pdf&quot;, &quot;application/pdf&quot;, pdfBytes).attach(&quot;me.png&quot;, &quot;image/png&quot;, pngBytes).send();
 * </pre>
 * 
 * @author Zach Melamed
 */
public class EmailSender
{
	private static final String USERNAME = "example@gmail.com"; // change accordingly
	private static final String PASSWORD = "********"; // change accordingly
	private static final String DEFAULT_FROM = "Zach <example@gmail.com>";
	private static final String DEFAULT_REPLY_TO = "noreply@gmail.com";

	/**
	 * fill this in with your own SMTP parameters
	 */
	private static Session getSession()
	{
		Properties props = new Properties();
		props.put("mail.smtp.auth", "true");
		props.put("mail.smtp.starttls.enable", "true");
		props.put("mail.smtp.host", "smtp.gmail.com");
		props.put("mail.smtp.port", "587");

		Session session = Session.getInstance(props, new Authenticator()
		{
			protected PasswordAuthentication getPasswordAuthentication()
			{
				return new PasswordAuthentication(USERNAME, PASSWORD);
			}
		});

		return session;
	}

	private MimeMessage msg = new MimeMessage(getSession());;
	private String mText;
	private String mHtml;
	private List<Attachment> mAttachments = new ArrayList<>();

	// TODO: support embedded attachments (using "related" multipart)
	// TODO: allow user to pass InputStream instead of byte array
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

	public static EmailSender subject(String subject) throws MessagingException
	{
		EmailSender sender = new EmailSender();
		sender.msg.setSubject(subject, "UTF-8");
		return sender.from(DEFAULT_FROM).replyTo(DEFAULT_REPLY_TO);
	}

	public EmailSender from(String from) throws AddressException, MessagingException
	{
		msg.setFrom(new InternetAddress(from));
		return this;
	}

	public EmailSender replyTo(String replyTo) throws AddressException, MessagingException
	{
		msg.setReplyTo(InternetAddress.parse(replyTo.replaceAll(";", ",")));
		return this;
	}

	public EmailSender to(String to) throws AddressException, MessagingException
	{
		msg.addRecipients(Message.RecipientType.TO, InternetAddress.parse(to.replaceAll(";", ",")));
		return this;
	}

	public EmailSender cc(String cc) throws AddressException, MessagingException
	{
		msg.addRecipients(Message.RecipientType.CC, InternetAddress.parse(cc.replaceAll(";", ",")));
		return this;
	}

	public EmailSender bcc(String bcc) throws AddressException, MessagingException
	{
		msg.addRecipients(Message.RecipientType.BCC, InternetAddress.parse(bcc.replaceAll(";", ",")));
		return this;
	}

	public EmailSender text(String text) throws MessagingException
	{
		mText = text;
		return this;
	}

	public EmailSender html(String html) throws MessagingException
	{
		mHtml = html;
		return this;
	}

	public EmailSender attach(String filename, String mimeType, byte[] data) throws MessagingException
	{
		mAttachments.add(new Attachment(filename, mimeType, data));
		return this;
	}

	public void send() throws MessagingException
	{
		if (mText == null && mHtml == null)
			throw new NullPointerException("At least one context has to be provided: Text or Html");

		MimeMultipart cover;
		boolean usingAlternative = false;
		boolean hasAttachments = mAttachments.size() > 0;

		if (mText != null && mHtml == null)
		{
			// TEXT ONLY
			cover = new MimeMultipart("mixed");
			cover.addBodyPart(textPart());
		}
		else if (mText == null && mHtml != null)
		{
			// HTML ONLY
			cover = new MimeMultipart("mixed");
			cover.addBodyPart(htmlPart());
		}
		else
		{
			// HTML + TEXT
			cover = new MimeMultipart("alternative");
			cover.addBodyPart(textPart());
			cover.addBodyPart(htmlPart());
			usingAlternative = true;
		}

		MimeMultipart content = cover;

		if (usingAlternative && hasAttachments)
		{
			content = new MimeMultipart("mixed");
			content.addBodyPart(toBodyPart(cover));
		}

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

	private MimeBodyPart toBodyPart(MimeMultipart cover) throws MessagingException
	{
		MimeBodyPart wrap = new MimeBodyPart();
		wrap.setContent(cover);
		return wrap;
	}

	private MimeBodyPart textPart() throws MessagingException
	{
		MimeBodyPart text = new MimeBodyPart();
		text.setText(mText);
		return text;
	}

	private MimeBodyPart htmlPart() throws MessagingException
	{
		MimeBodyPart html = new MimeBodyPart();
		html.setContent(mHtml, "text/html; charset=utf-8");
		return html;
	}
}
