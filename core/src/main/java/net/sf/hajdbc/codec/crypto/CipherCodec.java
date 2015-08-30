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
package net.sf.hajdbc.codec.crypto;

import java.security.GeneralSecurityException;
import java.security.Key;
import java.sql.SQLException;
import java.util.Base64;

import javax.crypto.Cipher;

import net.sf.hajdbc.codec.Codec;

/**
 * Generic cryptographic codec.
 * @author Paul Ferraro
 */
public class CipherCodec implements Codec
{
	private final Key key;
	
	public CipherCodec(Key key)
	{
		this.key = key;
	}
	
	public Key getKey()
	{
		return this.key;
	}
	
	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.codec.Codec#decode(java.lang.String)
	 */
	@Override
	public String decode(String value) throws SQLException
	{
		try
		{
			Cipher cipher = Cipher.getInstance(this.key.getAlgorithm());

			cipher.init(Cipher.DECRYPT_MODE, this.key);
			
			return new String(cipher.doFinal(Base64.getDecoder().decode(value)));
		}
		catch (GeneralSecurityException e)
		{
			throw new SQLException(e);
		}
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.codec.Codec#encode(java.lang.String)
	 */
	@Override
	public String encode(String value) throws SQLException
	{
		try
		{
			Cipher cipher = Cipher.getInstance(this.key.getAlgorithm());

			cipher.init(Cipher.ENCRYPT_MODE, this.key);
			
			return Base64.getEncoder().encodeToString(cipher.doFinal(value.getBytes()));
		}
		catch (GeneralSecurityException e)
		{
			throw new SQLException(e);
		}
	}
}
