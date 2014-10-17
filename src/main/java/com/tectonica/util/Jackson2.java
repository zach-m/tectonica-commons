package com.tectonica.util;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.Writer;

import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.io.JsonStringEncoder;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;

public class Jackson2
{
	private static final ObjectMapper treeMapper = new ObjectMapper();

	public static JsonNode parse(String jsonObject)
	{
		try
		{
			return treeMapper.readValue(jsonObject, JsonNode.class);
		}
		catch (Exception e)
		{
			throw new RuntimeException(e);
		}
	}

	public static ArrayNode parseArray(String jsonArray)
	{
		try
		{
			return treeMapper.readValue(jsonArray, ArrayNode.class);
		}
		catch (Exception e)
		{
			throw new RuntimeException(e);
		}
	}

	// ///////////////////////////////////////////////////////////////////

	public static String escape(String text)
	{
		return new String(JsonStringEncoder.getInstance().quoteAsUTF8(text));
	}

	// ///////////////////////////////////////////////////////////////////

	private static final ObjectMapper fieldsMapper = createFieldsMapper();
	private static final ObjectMapper propsMapper = createPropsMapper();

	public static ObjectMapper createFieldsMapper()
	{
		ObjectMapper mapper = new ObjectMapper();

		// limit to fields only
		mapper.setVisibility(PropertyAccessor.FIELD, Visibility.ANY);
		mapper.setVisibility(PropertyAccessor.GETTER, Visibility.NONE);
		mapper.setVisibility(PropertyAccessor.SETTER, Visibility.NONE);

		// general configuration
		mapper.setSerializationInclusion(Include.NON_NULL);
		mapper.enable(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY);

		return mapper;
	}

	public static ObjectMapper createPropsMapper()
	{
		ObjectMapper mapper = new ObjectMapper();

		// limit to props only
		mapper.setVisibility(PropertyAccessor.FIELD, Visibility.NONE);
		mapper.setVisibility(PropertyAccessor.GETTER, Visibility.ANY);
		mapper.setVisibility(PropertyAccessor.SETTER, Visibility.ANY);

		// general configuration
		mapper.setSerializationInclusion(Include.NON_NULL);
		mapper.enable(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY);

		return mapper;
	}

	// ///////////////////////////////////////////////////////////////////

	public static String toJson(Object o, ObjectMapper mapper)
	{
		try
		{
			return mapper.writeValueAsString(o);
		}
		catch (IOException e)
		{
			throw new RuntimeException(e);
		}
	}

	public static void toJson(OutputStream os, Object o, ObjectMapper mapper)
	{
		try
		{
			mapper.writeValue(os, o);
		}
		catch (IOException e)
		{
			throw new RuntimeException(e);
		}
	}

	public static void toJson(Writer w, Object o, ObjectMapper mapper)
	{
		try
		{
			mapper.writeValue(w, o);
		}
		catch (IOException e)
		{
			throw new RuntimeException(e);
		}
	}

	public static <T> T fromJson(String jsonStr, Class<T> clz, ObjectMapper mapper)
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

	public static <T> T fromJson(InputStream is, Class<T> clz, ObjectMapper mapper)
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

	public static <T> T fromJson(Reader r, Class<T> clz, ObjectMapper mapper)
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

	// ///////////////////////////////////////////////////////////////////

	public static String propsToJson(Object o)
	{
		return toJson(o, propsMapper);
	}

	public static void propsToJson(OutputStream os, Object o)
	{
		toJson(os, o, propsMapper);
	}

	public static void propsToJson(Writer w, Object o)
	{
		toJson(w, o, propsMapper);
	}

	public static <T> T propsFromJson(String jsonStr, Class<T> clz)
	{
		return fromJson(jsonStr, clz, propsMapper);
	}

	public static <T> T propsFromJson(InputStream is, Class<T> clz)
	{
		return fromJson(is, clz, propsMapper);
	}

	public static <T> T propsFromJson(Reader r, Class<T> clz)
	{
		return fromJson(r, clz, propsMapper);
	}

	public static String fieldsToJson(Object o)
	{
		return toJson(o, fieldsMapper);
	}

	public static void fieldsToJson(OutputStream os, Object o)
	{
		toJson(os, o, fieldsMapper);
	}

	public static void fieldsToJson(Writer w, Object o)
	{
		toJson(w, o, fieldsMapper);
	}

	public static <T> T fieldsFromJson(String jsonStr, Class<T> clz)
	{
		return fromJson(jsonStr, clz, fieldsMapper);
	}

	public static <T> T fieldsFromJson(InputStream is, Class<T> clz)
	{
		return fromJson(is, clz, fieldsMapper);
	}

	public static <T> T fieldsFromJson(Reader r, Class<T> clz)
	{
		return fromJson(r, clz, fieldsMapper);
	}
}
