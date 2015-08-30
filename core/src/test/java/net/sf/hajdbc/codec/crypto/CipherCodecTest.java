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

import java.security.Key;
import java.sql.SQLException;
import java.util.Base64;

import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.DESKeySpec;

import net.sf.hajdbc.codec.Codec;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * @author Paul Ferraro
 */
public class CipherCodecTest
{
	private static final String KEY = "x4zOovfg7+Y=";
	private static final String ALGORITHM = "DES";
	private static final String ENCODED = "wzAkF0hlYUeGhfzRQIxYAQ==";
	private static final String PASSWORD = "password";
	
	private Codec codec;
	
	@Before
	public void before() throws Exception
	{
		SecretKeyFactory factory = SecretKeyFactory.getInstance(ALGORITHM);
		Key key = factory.generateSecret(new DESKeySpec(Base64.getDecoder().decode(KEY)));
		
		this.codec = new CipherCodec(key);
	}
	
	@Test
	public void encode() throws SQLException
	{
		Assert.assertEquals(ENCODED, this.codec.encode(PASSWORD));
	}
	
	@Test
	public void decode() throws SQLException
	{
		Assert.assertEquals(PASSWORD, this.codec.decode(ENCODED));
	}
}
