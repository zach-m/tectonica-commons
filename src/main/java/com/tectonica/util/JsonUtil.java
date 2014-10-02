package com.tectonica.util;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.Writer;

import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

public class JsonUtil
{
	private static final ObjectMapper mapper = createFieldsMapper();

	public static ObjectMapper createFieldsMapper()
	{
		ObjectMapper mapper = new ObjectMapper();
		mapper.enable(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY);
		mapper.setVisibility(PropertyAccessor.FIELD, Visibility.ANY);
		mapper.setVisibility(PropertyAccessor.GETTER, Visibility.NONE);
		mapper.setVisibility(PropertyAccessor.SETTER, Visibility.NONE);
		mapper.setSerializationInclusion(Include.NON_NULL);
		return mapper;
	}

//	private static final ObjectMapper mapper = createJaxbMapper();
//	public static ObjectMapper createJaxbMapper()
//	{
//		ObjectMapper mapper = new ObjectMapper();
//		mapper.registerModule(new JaxbAnnotationModule());
//		mapper.enable(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY);
//		mapper.setSerializationInclusion(Include.NON_NULL);
//		mapper.registerModule(new JodaModule());
//		return mapper;
//	}

	public static ObjectMapper mapper()
	{
		return mapper;
	}

	public static String toJson(Object o)
	{
		try
		{
			if (mapper.canSerialize(o.getClass()))
				return (mapper.writeValueAsString(o));
		}
		catch (JsonProcessingException e)
		{
			throw new RuntimeException(e);
		}
		throw new RuntimeException("Unserializable object: " + o.toString());
	}

	public static void toJson(OutputStream os, Object o)
	{
		try
		{
			if (mapper.canSerialize(o.getClass()))
			{
				mapper.writeValue(os, o);
				return;
			}
		}
		catch (IOException e)
		{
			throw new RuntimeException(e);
		}
		throw new RuntimeException("Unserializable object: " + o.toString());
	}

	public static void toJson(Writer w, Object o)
	{
		try
		{
			if (mapper.canSerialize(o.getClass()))
			{
				mapper.writeValue(w, o);
				return;
			}
		}
		catch (IOException e)
		{
			throw new RuntimeException(e);
		}
		throw new RuntimeException("Unserializable object: " + o.toString());
	}

	public static <T> T fromJson(String jsonStr, Class<T> clz)
	{
		try
		{
			return mapper.readValue(jsonStr, clz);
		}
		catch (IOException e)
		{
			throw new RuntimeException(e);
		}
	}

	public static <T> T fromJson(InputStream is, Class<T> clz)
	{
		try
		{
			return mapper.readValue(is, clz);
		}
		catch (IOException e)
		{
			throw new RuntimeException(e);
		}
	}

	public static <T> T fromJson(Reader r, Class<T> clz)
	{
		try
		{
			return mapper.readValue(r, clz);
		}
		catch (IOException e)
		{
			throw new RuntimeException(e);
		}
	}
}
