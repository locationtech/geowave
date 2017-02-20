package mil.nga.giat.geowave.core.store.memory;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.log4j.Logger;

public class MemoryStoreUtils
{
	private final static Logger LOGGER = Logger.getLogger(MemoryStoreUtils.class);

	protected static boolean isAuthorized(
			final byte[] visibility,
			final String[] authorizations ) {
		if ((visibility == null) || (visibility.length == 0)) {
			return true;
		}
		VisibilityExpression expr;
		try {
			expr = new VisibilityExpressionParser().parse(visibility);
		}
		catch (final IOException e) {
			LOGGER.error(
					"invalid visibility",
					e);
			return false;
		}
		return expr.ok(authorizations);
	}

	private abstract static class VisibilityExpression
	{

		public abstract boolean ok(
				String[] auths );

		public VisibilityExpression and() {
			final AndExpression exp = new AndExpression();
			exp.add(this);
			return exp;
		}

		public VisibilityExpression or() {
			final OrExpression exp = new OrExpression();
			exp.add(this);
			return exp;
		}

		public abstract List<VisibilityExpression> children();

		public abstract VisibilityExpression add(
				VisibilityExpression expression );

	}

	public static enum NodeType {
		TERM,
		OR,
		AND,
	}

	private static class VisibilityExpressionParser
	{
		private int index = 0;
		private int parens = 0;

		public VisibilityExpressionParser() {}

		VisibilityExpression parse(
				final byte[] expression )
				throws IOException {
			if (expression.length > 0) {
				final VisibilityExpression expr = parse_(expression);
				if (expr == null) {
					badArgumentException(
							"operator or missing parens",
							expression,
							index - 1);
				}
				if (parens != 0) {
					badArgumentException(
							"parenthesis mis-match",
							expression,
							index - 1);
				}
				return expr;
			}
			return null;
		}

		VisibilityExpression processTerm(
				final int start,
				final int end,
				final VisibilityExpression expr,
				final byte[] expression )
				throws UnsupportedEncodingException {
			if (start != end) {
				if (expr != null) {
					badArgumentException(
							"expression needs | or &",
							expression,
							start);
				}
				return new ChildExpression(
						new String(
								Arrays.copyOfRange(
										expression,
										start,
										end),
								"UTF-8"));
			}
			if (expr == null) {
				badArgumentException(
						"empty term",
						Arrays.copyOfRange(
								expression,
								start,
								end),
						start);
			}
			return expr;
		}

		VisibilityExpression parse_(
				final byte[] expression )
				throws IOException {
			VisibilityExpression result = null;
			VisibilityExpression expr = null;
			int termStart = index;
			while (index < expression.length) {
				switch (expression[index++]) {
					case '&': {
						expr = processTerm(
								termStart,
								index - 1,
								expr,
								expression);
						if (result != null) {
							if (!(result instanceof AndExpression)) {
								badArgumentException(
										"cannot mix & and |",
										expression,
										index - 1);
							}
						}
						else {
							result = new AndExpression();
						}
						result.add(expr);
						expr = null;
						termStart = index;
						break;
					}
					case '|': {
						expr = processTerm(
								termStart,
								index - 1,
								expr,
								expression);
						if (result != null) {
							if (!(result instanceof OrExpression)) {
								badArgumentException(
										"cannot mix | and &",
										expression,
										index - 1);
							}
						}
						else {
							result = new OrExpression();
						}
						result.add(expr);
						expr = null;
						termStart = index;
						break;
					}
					case '(': {
						parens++;
						if ((termStart != (index - 1)) || (expr != null)) {
							badArgumentException(
									"expression needs & or |",
									expression,
									index - 1);
						}
						expr = parse_(expression);
						termStart = index;
						break;
					}
					case ')': {
						parens--;
						final VisibilityExpression child = processTerm(
								termStart,
								index - 1,
								expr,
								expression);
						if ((child == null) && (result == null)) {
							badArgumentException(
									"empty expression not allowed",
									expression,
									index);
						}
						if (result == null) {
							return child;
						}
						result.add(child);
						return result;
					}
				}
			}
			final VisibilityExpression child = processTerm(
					termStart,
					index,
					expr,
					expression);
			if (result != null) {
				result.add(child);
			}
			else {
				result = child;
			}
			if (!(result instanceof ChildExpression)) {
				if (result.children().size() < 2) {
					badArgumentException(
							"missing term",
							expression,
							index);
				}
			}
			return result;
		}
	}

	public abstract static class CompositeExpression extends
			VisibilityExpression
	{
		protected final List<VisibilityExpression> expressions = new ArrayList<VisibilityExpression>();

		@Override
		public VisibilityExpression add(
				final VisibilityExpression expression ) {
			if (expression.getClass().equals(
					this.getClass())) {
				for (final VisibilityExpression child : expression.children()) {
					add(child);
				}
			}
			else {
				expressions.add(expression);
			}
			return this;
		}
	}

	public static class ChildExpression extends
			VisibilityExpression
	{
		private final String value;

		public ChildExpression(
				final String value ) {
			super();
			this.value = value;
		}

		@Override
		public boolean ok(
				final String[] auths ) {
			if (auths != null) {
				for (final String auth : auths) {
					if (value.equals(auth)) {
						return true;
					}
				}
			}
			return false;
		}

		@Override
		public List<VisibilityExpression> children() {
			return Collections.emptyList();
		}

		@Override
		public VisibilityExpression add(
				final VisibilityExpression expression ) {
			return this;
		}
	}

	public static class AndExpression extends
			CompositeExpression
	{

		@Override
		public List<VisibilityExpression> children() {
			return expressions;
		}

		@Override
		public boolean ok(
				final String[] auth ) {
			for (final VisibilityExpression expression : expressions) {
				if (!expression.ok(auth)) {
					return false;
				}
			}
			return true;
		}

		public VisibilityExpression and(
				final VisibilityExpression expression ) {
			return this;
		}
	}

	public static class OrExpression extends
			CompositeExpression
	{

		@Override
		public boolean ok(
				final String[] auths ) {
			for (final VisibilityExpression expression : expressions) {
				if (expression.ok(auths)) {
					return true;
				}
			}
			return false;
		}

		@Override
		public List<VisibilityExpression> children() {
			return expressions;
		}

		public VisibilityExpression or(
				final VisibilityExpression expression ) {
			return this;
		}

	}

	private static final void badArgumentException(
			final String msg,
			final byte[] expression,
			final int place ) {
		throw new IllegalArgumentException(
				msg + " for " + Arrays.toString(expression) + " at " + place);
	}
}
