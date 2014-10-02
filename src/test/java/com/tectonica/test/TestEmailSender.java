package com.tectonica.test;

import java.io.UnsupportedEncodingException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.mail.MessagingException;

import org.junit.Test;
import com.tectonica.util.EmailSender;

public class TestEmailSender
{
	private static Logger LOG = Logger.getLogger(TestEmailSender.class.getSimpleName());

	@Test
	public void test() throws UnsupportedEncodingException
	{
		SimpleDateFormat sdf = new SimpleDateFormat("dd-MM-yy HH:mm:ss.SSS");
		String to = "zach@bringoz.com";
		String text = "If you see this message, we're all good";

		try
		{
			String html = String.format("<html><body><h1>%s</h1>%s</body></html>", "Html Header", text);
			byte[] data1 = "This is the contents of attachment 1, say a spreadsheet".getBytes("UTF-8");
			byte[] data2 = "This is the contents of attachment 2, say an image".getBytes("UTF-8");
			String title;

			title = "[TEXT ONLY] " + sdf.format(new Date());
			LOG.info("Sending '" + title + "' to " + to);
			EmailSender.subject(title).to(to).text(text).send();

			title = "[HTML ONLY] " + sdf.format(new Date());
			LOG.info("Sending '" + title + "' to " + to);
			EmailSender.subject(title).to(to).html(html).send();

			title = "[HTML+TEXT] " + sdf.format(new Date());
			LOG.info("Sending '" + title + "' to " + to);
			EmailSender.subject(title).to(to).html(html).text(text).send();

			title = "[TEXT ONLY + ATTACH] " + sdf.format(new Date());
			LOG.info("Sending '" + title + "' to " + to);
			EmailSender.subject(title).to(to).text(text).attach("t1.txt", "text/plain", data1).attach("t2.txt", "text/plain", data2).send();

			title = "[HTML ONLY + ATTACH] " + sdf.format(new Date());
			LOG.info("Sending '" + title + "' to " + to);
			EmailSender.subject(title).to(to).html(html).attach("t1.txt", "text/plain", data1).attach("t2.txt", "text/plain", data2).send();

			title = "[HTML+TEXT + ATTACH] " + sdf.format(new Date());
			LOG.info("Sending '" + title + "' to " + to);
			EmailSender.subject(title).to(to).html(html).text(text).attach("t1.txt", "text/plain", data1)
					.attach("t2.txt", "text/plain", data2).send();
		}
		catch (MessagingException e)
		{
			LOG.log(Level.SEVERE, "SEND ERROR", e);
		}
	}
}
