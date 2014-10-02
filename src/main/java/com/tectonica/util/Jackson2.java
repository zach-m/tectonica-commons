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
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;

public class Jackson2
{
	private static final ObjectMapper treeMapper = new ObjectMapper();
	private static final ObjectMapper fieldsMapper = createFieldsMapper();

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

	public static ObjectMapper fieldsMapper()
	{
		return fieldsMapper;
	}

	public static String toJson(Object o)
	{
		try
		{
			if (fieldsMapper.canSerialize(o.getClass()))
				return (fieldsMapper.writeValueAsString(o));
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
			if (fieldsMapper.canSerialize(o.getClass()))
			{
				fieldsMapper.writeValue(os, o);
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
			if (fieldsMapper.canSerialize(o.getClass()))
			{
				fieldsMapper.writeValue(w, o);
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
			return fieldsMapper.readValue(jsonStr, clz);
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
			return fieldsMapper.readValue(is, clz);
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
			return fieldsMapper.readValue(r, clz);
		}
		catch (IOException e)
		{
			throw new RuntimeException(e);
		}
	}

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
}
