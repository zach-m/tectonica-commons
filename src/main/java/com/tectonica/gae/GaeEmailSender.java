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

public class GaeEmailSender
{
	/**
	 * this email-address must be listed in the GAE application Permissions area
	 */
//	private static final String DEFAULT_FROM = "Example <" + System.getProperties().get("com.google.appengine.application.id")
//			+ "@appspot.gserviceaccount.com>";
	private static String defaultFrom = null; //"Example <jack@example.com>";
	
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
		return sender.from(defaultFrom).replyTo(defaultReplyTo);
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