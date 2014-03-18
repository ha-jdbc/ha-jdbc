package net.sf.hajdbc.util.concurrent.cron;

import java.text.ParseException;

import net.sf.hajdbc.configuration.Builder;

public class CronExpressionBuilder implements Builder<CronExpression>
{
	private volatile String expression;
	
	public CronExpressionBuilder expression(String expression)
	{
		this.expression = expression;
		return this;
	}
	
	@Override
	public CronExpressionBuilder read(CronExpression expression)
	{
		return this.expression(expression.getCronExpression());
	}

	@Override
	public CronExpression build()
	{
		String expression = this.expression;
		if (expression == null) return null;
		try
		{
			return new CronExpression(expression);
		}
		catch (ParseException e)
		{
			throw new IllegalArgumentException(e);
		}
	}
}
