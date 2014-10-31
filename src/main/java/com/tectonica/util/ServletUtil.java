package com.tectonica.util;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public class ServletUtil
{
	public static void applyCORS(HttpServletRequest req, HttpServletResponse resp)
	{
		resp.setHeader("Access-Control-Allow-Origin", "*");
		resp.setHeader("Access-Control-Allow-Methods", "GET, POST, OPTIONS");

		String acrh = req.getHeader("Access-Control-Request-Headers");
		if (acrh != null && !acrh.isEmpty())
			resp.setHeader("Access-Control-Allow-Headers", acrh);
	}
}
