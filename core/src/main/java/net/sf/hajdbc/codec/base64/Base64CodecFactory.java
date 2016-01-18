/*
 * HA-JDBC: High-Availability JDBC
 * Copyright (C) 2012  Paul Ferraro
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package net.sf.hajdbc.codec.base64;

import java.sql.SQLException;
import java.util.Base64;

import net.sf.hajdbc.codec.AbstractCodec;
import net.sf.hajdbc.codec.Codec;
import net.sf.hajdbc.util.Strings;

/**
 * Codec that uses base-64 encoding/decoding.
 * @author Paul Ferraro
 */
public class Base64CodecFactory extends AbstractCodec
{
	private static final long serialVersionUID = -2286529406290006597L;

	@Override
	public String getId()
	{
		return "64";
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.codec.Codec#decode(java.lang.String)
	 */
	@Override
	public String decode(String value)
	{
		return new String(Base64.getDecoder().decode(value));
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.codec.Codec#encode(java.lang.String)
	 */
	@Override
	public String encode(String value)
	{
		return Base64.getEncoder().encodeToString(value.getBytes());
	}
	
	public static void main(String... args)
	{
		if (args.length != 2)
		{
			System.err.println(String.format("Usage:%s\tjava %s <cluster-id> <password-to-encrypt>", Strings.NEW_LINE, Base64CodecFactory.class.getName()));
			System.exit(1);
			return;
		}
		
		String clusterId = args[0];
		String value = args[1];
		
		try
		{
			Codec codec = new Base64CodecFactory().createCodec(clusterId);

			System.out.println(codec.encode(value));
		}
		catch (SQLException e)
		{
			e.printStackTrace(System.err);
		}
	}
}
