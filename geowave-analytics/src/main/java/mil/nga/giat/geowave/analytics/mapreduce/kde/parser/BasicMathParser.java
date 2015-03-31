package mil.nga.giat.geowave.analytics.mapreduce.kde.parser;

// $ANTLR 3.3 BasicMath.g 2014-05-13 05:58:44

import java.util.ArrayList;
import java.util.List;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.antlr.runtime.BitSet;
import org.antlr.runtime.MismatchedSetException;
import org.antlr.runtime.NoViableAltException;
import org.antlr.runtime.Parser;
import org.antlr.runtime.ParserRuleReturnScope;
import org.antlr.runtime.RecognitionException;
import org.antlr.runtime.RecognizerSharedState;
import org.antlr.runtime.Token;
import org.antlr.runtime.TokenStream;
import org.antlr.runtime.tree.CommonTree;
import org.antlr.runtime.tree.CommonTreeAdaptor;
import org.antlr.runtime.tree.RewriteRuleSubtreeStream;
import org.antlr.runtime.tree.RewriteRuleTokenStream;
import org.antlr.runtime.tree.TreeAdaptor;

@SuppressFBWarnings({
	"RCN_REDUNDANT_NULLCHECK_OF_NONNULL_VALUE",
	"SF_SWITCH_NO_DEFAULT",
	"DLS_DEAD_LOCAL_STORE"
})
@SuppressWarnings("all")
public class BasicMathParser extends
		Parser
{
	public static final String[] tokenNames = new String[] {
		"<invalid>",
		"<EOR>",
		"<DOWN>",
		"<UP>",
		"AND",
		"BOOLEAN",
		"DATETIME",
		"DIV",
		"EQUALS",
		"EscapeSequence",
		"FLOAT",
		"GT",
		"GTEQ",
		"HexDigit",
		"IDENT",
		"INTEGER",
		"LT",
		"LTEQ",
		"MINUS",
		"MOD",
		"MULT",
		"NEGATE",
		"NOT",
		"NOTEQUALS",
		"OR",
		"PARAM",
		"PLUS",
		"POW",
		"STRING",
		"UnicodeEscape",
		"WS",
		"'('",
		"')'",
		"','",
		"'['",
		"']'"
	};
	public static final int EOF = -1;
	public static final int T__31 = 31;
	public static final int T__32 = 32;
	public static final int T__33 = 33;
	public static final int T__34 = 34;
	public static final int T__35 = 35;
	public static final int AND = 4;
	public static final int BOOLEAN = 5;
	public static final int DATETIME = 6;
	public static final int DIV = 7;
	public static final int EQUALS = 8;
	public static final int EscapeSequence = 9;
	public static final int FLOAT = 10;
	public static final int GT = 11;
	public static final int GTEQ = 12;
	public static final int HexDigit = 13;
	public static final int IDENT = 14;
	public static final int INTEGER = 15;
	public static final int LT = 16;
	public static final int LTEQ = 17;
	public static final int MINUS = 18;
	public static final int MOD = 19;
	public static final int MULT = 20;
	public static final int NEGATE = 21;
	public static final int NOT = 22;
	public static final int NOTEQUALS = 23;
	public static final int OR = 24;
	public static final int PARAM = 25;
	public static final int PLUS = 26;
	public static final int POW = 27;
	public static final int STRING = 28;
	public static final int UnicodeEscape = 29;
	public static final int WS = 30;

	// delegates
	public Parser[] getDelegates() {
		return new Parser[] {};
	}

	// delegators

	public BasicMathParser(
			final TokenStream input ) {
		this(
				input,
				new RecognizerSharedState());
	}

	public BasicMathParser(
			final TokenStream input,
			final RecognizerSharedState state ) {
		super(
				input,
				state);
	}

	protected TreeAdaptor adaptor = new CommonTreeAdaptor();

	public void setTreeAdaptor(
			final TreeAdaptor adaptor ) {
		this.adaptor = adaptor;
	}

	public TreeAdaptor getTreeAdaptor() {
		return adaptor;
	}

	@Override
	public String[] getTokenNames() {
		return BasicMathParser.tokenNames;
	}

	@Override
	public String getGrammarFileName() {
		return "ECalc.g";
	}

	List<RecognitionException> exceptions = new ArrayList<RecognitionException>();

	@Override
	public void reportError(
			final RecognitionException e ) {
		exceptions.add(e);
	}

	public boolean hasError() {
		return !exceptions.isEmpty();
	}

	public String errorMessage() {
		return getErrorMessage(
				exceptions.get(0),
				tokenNames);
	}

	public String errorPosition() {
		return getErrorHeader(exceptions.get(0));
	}

	public static class expression_return extends
			ParserRuleReturnScope
	{
		CommonTree tree;

		@Override
		public CommonTree getTree() {
			return tree;
		}
	};

	// $ANTLR start "expression"
	// ECalc.g:45:1: expression : logicalExpression EOF !;
	public final BasicMathParser.expression_return expression()
			throws RecognitionException {
		final BasicMathParser.expression_return retval = new BasicMathParser.expression_return();
		retval.start = input.LT(1);

		CommonTree root_0 = null;

		Token EOF2 = null;
		ParserRuleReturnScope logicalExpression1 = null;

		final CommonTree EOF2_tree = null;

		try {
			// ECalc.g:46:2: ( logicalExpression EOF !)
			// ECalc.g:46:5: logicalExpression EOF !
			{
				root_0 = (CommonTree) adaptor.nil();

				pushFollow(FOLLOW_logicalExpression_in_expression64);
				logicalExpression1 = logicalExpression();
				state._fsp--;

				adaptor.addChild(
						root_0,
						logicalExpression1.getTree());

				EOF2 = (Token) match(
						input,
						EOF,
						FOLLOW_EOF_in_expression66);
			}

			retval.stop = input.LT(-1);

			retval.tree = (CommonTree) adaptor.rulePostProcessing(root_0);
			adaptor.setTokenBoundaries(
					retval.tree,
					retval.start,
					retval.stop);

		}
		catch (final RecognitionException re) {
			reportError(re);
			recover(
					input,
					re);
			retval.tree = (CommonTree) adaptor.errorNode(
					input,
					retval.start,
					input.LT(-1),
					re);
		}
		finally {
			// do for sure before leaving
		}
		return retval;
	}

	// $ANTLR end "expression"

	public static class logicalExpression_return extends
			ParserRuleReturnScope
	{
		CommonTree tree;

		@Override
		public CommonTree getTree() {
			return tree;
		}
	};

	// $ANTLR start "logicalExpression"
	// ECalc.g:49:1: logicalExpression : booleanAndExpression ( OR ^
	// booleanAndExpression )* ;
	public final BasicMathParser.logicalExpression_return logicalExpression()
			throws RecognitionException {
		final BasicMathParser.logicalExpression_return retval = new BasicMathParser.logicalExpression_return();
		retval.start = input.LT(1);

		CommonTree root_0 = null;

		Token OR4 = null;
		ParserRuleReturnScope booleanAndExpression3 = null;
		ParserRuleReturnScope booleanAndExpression5 = null;

		CommonTree OR4_tree = null;

		try {
			// ECalc.g:50:2: ( booleanAndExpression ( OR ^ booleanAndExpression
			// )* )
			// ECalc.g:50:4: booleanAndExpression ( OR ^ booleanAndExpression )*
			{
				root_0 = (CommonTree) adaptor.nil();

				pushFollow(FOLLOW_booleanAndExpression_in_logicalExpression80);
				booleanAndExpression3 = booleanAndExpression();
				state._fsp--;

				adaptor.addChild(
						root_0,
						booleanAndExpression3.getTree());

				// ECalc.g:50:25: ( OR ^ booleanAndExpression )*
				loop1: while (true) {
					int alt1 = 2;
					final int LA1_0 = input.LA(1);
					if ((LA1_0 == OR)) {
						alt1 = 1;
					}

					switch (alt1) {
						case 1:
						// ECalc.g:50:26: OR ^ booleanAndExpression
						{
							OR4 = (Token) match(
									input,
									OR,
									FOLLOW_OR_in_logicalExpression83);
							OR4_tree = (CommonTree) adaptor.create(OR4);
							root_0 = (CommonTree) adaptor.becomeRoot(
									OR4_tree,
									root_0);

							pushFollow(FOLLOW_booleanAndExpression_in_logicalExpression86);
							booleanAndExpression5 = booleanAndExpression();
							state._fsp--;

							adaptor.addChild(
									root_0,
									booleanAndExpression5.getTree());

						}
							break;

						default:
							break loop1;
					}
				}

			}

			retval.stop = input.LT(-1);

			retval.tree = (CommonTree) adaptor.rulePostProcessing(root_0);
			adaptor.setTokenBoundaries(
					retval.tree,
					retval.start,
					retval.stop);

		}
		catch (final RecognitionException re) {
			reportError(re);
			recover(
					input,
					re);
			retval.tree = (CommonTree) adaptor.errorNode(
					input,
					retval.start,
					input.LT(-1),
					re);
		}
		finally {
			// do for sure before leaving
		}
		return retval;
	}

	// $ANTLR end "logicalExpression"

	public static class booleanAndExpression_return extends
			ParserRuleReturnScope
	{
		CommonTree tree;

		@Override
		public CommonTree getTree() {
			return tree;
		}
	};

	// $ANTLR start "booleanAndExpression"
	// ECalc.g:55:1: booleanAndExpression : equalityExpression ( AND ^
	// equalityExpression )* ;
	public final BasicMathParser.booleanAndExpression_return booleanAndExpression()
			throws RecognitionException {
		final BasicMathParser.booleanAndExpression_return retval = new BasicMathParser.booleanAndExpression_return();
		retval.start = input.LT(1);

		CommonTree root_0 = null;

		Token AND7 = null;
		ParserRuleReturnScope equalityExpression6 = null;
		ParserRuleReturnScope equalityExpression8 = null;

		CommonTree AND7_tree = null;

		try {
			// ECalc.g:56:2: ( equalityExpression ( AND ^ equalityExpression )*
			// )
			// ECalc.g:56:4: equalityExpression ( AND ^ equalityExpression )*
			{
				root_0 = (CommonTree) adaptor.nil();

				pushFollow(FOLLOW_equalityExpression_in_booleanAndExpression116);
				equalityExpression6 = equalityExpression();
				state._fsp--;

				adaptor.addChild(
						root_0,
						equalityExpression6.getTree());

				// ECalc.g:56:23: ( AND ^ equalityExpression )*
				loop2: while (true) {
					int alt2 = 2;
					final int LA2_0 = input.LA(1);
					if ((LA2_0 == AND)) {
						alt2 = 1;
					}

					switch (alt2) {
						case 1:
						// ECalc.g:56:24: AND ^ equalityExpression
						{
							AND7 = (Token) match(
									input,
									AND,
									FOLLOW_AND_in_booleanAndExpression119);
							AND7_tree = (CommonTree) adaptor.create(AND7);
							root_0 = (CommonTree) adaptor.becomeRoot(
									AND7_tree,
									root_0);

							pushFollow(FOLLOW_equalityExpression_in_booleanAndExpression122);
							equalityExpression8 = equalityExpression();
							state._fsp--;

							adaptor.addChild(
									root_0,
									equalityExpression8.getTree());

						}
							break;

						default:
							break loop2;
					}
				}

			}

			retval.stop = input.LT(-1);

			retval.tree = (CommonTree) adaptor.rulePostProcessing(root_0);
			adaptor.setTokenBoundaries(
					retval.tree,
					retval.start,
					retval.stop);

		}
		catch (final RecognitionException re) {
			reportError(re);
			recover(
					input,
					re);
			retval.tree = (CommonTree) adaptor.errorNode(
					input,
					retval.start,
					input.LT(-1),
					re);
		}
		finally {
			// do for sure before leaving
		}
		return retval;
	}

	// $ANTLR end "booleanAndExpression"

	public static class equalityExpression_return extends
			ParserRuleReturnScope
	{
		CommonTree tree;

		@Override
		public CommonTree getTree() {
			return tree;
		}
	};

	// $ANTLR start "equalityExpression"
	// ECalc.g:61:1: equalityExpression : relationalExpression ( ( EQUALS |
	// NOTEQUALS ) ^ relationalExpression )* ;
	public final BasicMathParser.equalityExpression_return equalityExpression()
			throws RecognitionException {
		final BasicMathParser.equalityExpression_return retval = new BasicMathParser.equalityExpression_return();
		retval.start = input.LT(1);

		CommonTree root_0 = null;

		Token set10 = null;
		ParserRuleReturnScope relationalExpression9 = null;
		ParserRuleReturnScope relationalExpression11 = null;

		final CommonTree set10_tree = null;

		try {
			// ECalc.g:62:2: ( relationalExpression ( ( EQUALS | NOTEQUALS ) ^
			// relationalExpression )* )
			// ECalc.g:62:4: relationalExpression ( ( EQUALS | NOTEQUALS ) ^
			// relationalExpression )*
			{
				root_0 = (CommonTree) adaptor.nil();

				pushFollow(FOLLOW_relationalExpression_in_equalityExpression150);
				relationalExpression9 = relationalExpression();
				state._fsp--;

				adaptor.addChild(
						root_0,
						relationalExpression9.getTree());

				// ECalc.g:62:25: ( ( EQUALS | NOTEQUALS ) ^
				// relationalExpression )*
				loop3: while (true) {
					int alt3 = 2;
					final int LA3_0 = input.LA(1);
					if (((LA3_0 == EQUALS) || (LA3_0 == NOTEQUALS))) {
						alt3 = 1;
					}

					switch (alt3) {
						case 1:
						// ECalc.g:62:26: ( EQUALS | NOTEQUALS ) ^
						// relationalExpression
						{
							set10 = input.LT(1);
							set10 = input.LT(1);
							if ((input.LA(1) == EQUALS) || (input.LA(1) == NOTEQUALS)) {
								input.consume();
								root_0 = (CommonTree) adaptor.becomeRoot(
										adaptor.create(set10),
										root_0);
								state.errorRecovery = false;
							}
							else {
								final MismatchedSetException mse = new MismatchedSetException(
										null,
										input);
								throw mse;
							}
							pushFollow(FOLLOW_relationalExpression_in_equalityExpression160);
							relationalExpression11 = relationalExpression();
							state._fsp--;

							adaptor.addChild(
									root_0,
									relationalExpression11.getTree());

						}
							break;

						default:
							break loop3;
					}
				}

			}

			retval.stop = input.LT(-1);

			retval.tree = (CommonTree) adaptor.rulePostProcessing(root_0);
			adaptor.setTokenBoundaries(
					retval.tree,
					retval.start,
					retval.stop);

		}
		catch (final RecognitionException re) {
			reportError(re);
			recover(
					input,
					re);
			retval.tree = (CommonTree) adaptor.errorNode(
					input,
					retval.start,
					input.LT(-1),
					re);
		}
		finally {
			// do for sure before leaving
		}
		return retval;
	}

	// $ANTLR end "equalityExpression"

	public static class relationalExpression_return extends
			ParserRuleReturnScope
	{
		CommonTree tree;

		@Override
		public CommonTree getTree() {
			return tree;
		}
	};

	// $ANTLR start "relationalExpression"
	// ECalc.g:70:1: relationalExpression : additiveExpression ( ( LT | LTEQ |
	// GT | GTEQ ) ^ additiveExpression )* ;
	public final BasicMathParser.relationalExpression_return relationalExpression()
			throws RecognitionException {
		final BasicMathParser.relationalExpression_return retval = new BasicMathParser.relationalExpression_return();
		retval.start = input.LT(1);

		CommonTree root_0 = null;

		Token set13 = null;
		ParserRuleReturnScope additiveExpression12 = null;
		ParserRuleReturnScope additiveExpression14 = null;

		final CommonTree set13_tree = null;

		try {
			// ECalc.g:71:2: ( additiveExpression ( ( LT | LTEQ | GT | GTEQ ) ^
			// additiveExpression )* )
			// ECalc.g:71:4: additiveExpression ( ( LT | LTEQ | GT | GTEQ ) ^
			// additiveExpression )*
			{
				root_0 = (CommonTree) adaptor.nil();

				pushFollow(FOLLOW_additiveExpression_in_relationalExpression200);
				additiveExpression12 = additiveExpression();
				state._fsp--;

				adaptor.addChild(
						root_0,
						additiveExpression12.getTree());

				// ECalc.g:71:23: ( ( LT | LTEQ | GT | GTEQ ) ^
				// additiveExpression )*
				loop4: while (true) {
					int alt4 = 2;
					final int LA4_0 = input.LA(1);
					if ((((LA4_0 >= GT) && (LA4_0 <= GTEQ)) || ((LA4_0 >= LT) && (LA4_0 <= LTEQ)))) {
						alt4 = 1;
					}

					switch (alt4) {
						case 1:
						// ECalc.g:71:25: ( LT | LTEQ | GT | GTEQ ) ^
						// additiveExpression
						{
							set13 = input.LT(1);
							set13 = input.LT(1);
							if (((input.LA(1) >= GT) && (input.LA(1) <= GTEQ)) || ((input.LA(1) >= LT) && (input.LA(1) <= LTEQ))) {
								input.consume();
								root_0 = (CommonTree) adaptor.becomeRoot(
										adaptor.create(set13),
										root_0);
								state.errorRecovery = false;
							}
							else {
								final MismatchedSetException mse = new MismatchedSetException(
										null,
										input);
								throw mse;
							}
							pushFollow(FOLLOW_additiveExpression_in_relationalExpression215);
							additiveExpression14 = additiveExpression();
							state._fsp--;

							adaptor.addChild(
									root_0,
									additiveExpression14.getTree());

						}
							break;

						default:
							break loop4;
					}
				}

			}

			retval.stop = input.LT(-1);

			retval.tree = (CommonTree) adaptor.rulePostProcessing(root_0);
			adaptor.setTokenBoundaries(
					retval.tree,
					retval.start,
					retval.stop);

		}
		catch (final RecognitionException re) {
			reportError(re);
			recover(
					input,
					re);
			retval.tree = (CommonTree) adaptor.errorNode(
					input,
					retval.start,
					input.LT(-1),
					re);
		}
		finally {
			// do for sure before leaving
		}
		return retval;
	}

	// $ANTLR end "relationalExpression"

	public static class additiveExpression_return extends
			ParserRuleReturnScope
	{
		CommonTree tree;

		@Override
		public CommonTree getTree() {
			return tree;
		}
	};

	// $ANTLR start "additiveExpression"
	// ECalc.g:79:1: additiveExpression : multiplicativeExpression ( ( PLUS |
	// MINUS ) ^ multiplicativeExpression )* ;
	public final BasicMathParser.additiveExpression_return additiveExpression()
			throws RecognitionException {
		final BasicMathParser.additiveExpression_return retval = new BasicMathParser.additiveExpression_return();
		retval.start = input.LT(1);

		CommonTree root_0 = null;

		Token set16 = null;
		ParserRuleReturnScope multiplicativeExpression15 = null;
		ParserRuleReturnScope multiplicativeExpression17 = null;

		final CommonTree set16_tree = null;

		try {
			// ECalc.g:80:2: ( multiplicativeExpression ( ( PLUS | MINUS ) ^
			// multiplicativeExpression )* )
			// ECalc.g:80:4: multiplicativeExpression ( ( PLUS | MINUS ) ^
			// multiplicativeExpression )*
			{
				root_0 = (CommonTree) adaptor.nil();

				pushFollow(FOLLOW_multiplicativeExpression_in_additiveExpression258);
				multiplicativeExpression15 = multiplicativeExpression();
				state._fsp--;

				adaptor.addChild(
						root_0,
						multiplicativeExpression15.getTree());

				// ECalc.g:80:29: ( ( PLUS | MINUS ) ^ multiplicativeExpression
				// )*
				loop5: while (true) {
					int alt5 = 2;
					final int LA5_0 = input.LA(1);
					if (((LA5_0 == MINUS) || (LA5_0 == PLUS))) {
						alt5 = 1;
					}

					switch (alt5) {
						case 1:
						// ECalc.g:80:31: ( PLUS | MINUS ) ^
						// multiplicativeExpression
						{
							set16 = input.LT(1);
							set16 = input.LT(1);
							if ((input.LA(1) == MINUS) || (input.LA(1) == PLUS)) {
								input.consume();
								root_0 = (CommonTree) adaptor.becomeRoot(
										adaptor.create(set16),
										root_0);
								state.errorRecovery = false;
							}
							else {
								final MismatchedSetException mse = new MismatchedSetException(
										null,
										input);
								throw mse;
							}
							pushFollow(FOLLOW_multiplicativeExpression_in_additiveExpression269);
							multiplicativeExpression17 = multiplicativeExpression();
							state._fsp--;

							adaptor.addChild(
									root_0,
									multiplicativeExpression17.getTree());

						}
							break;

						default:
							break loop5;
					}
				}

			}

			retval.stop = input.LT(-1);

			retval.tree = (CommonTree) adaptor.rulePostProcessing(root_0);
			adaptor.setTokenBoundaries(
					retval.tree,
					retval.start,
					retval.stop);

		}
		catch (final RecognitionException re) {
			reportError(re);
			recover(
					input,
					re);
			retval.tree = (CommonTree) adaptor.errorNode(
					input,
					retval.start,
					input.LT(-1),
					re);
		}
		finally {
			// do for sure before leaving
		}
		return retval;
	}

	// $ANTLR end "additiveExpression"

	public static class multiplicativeExpression_return extends
			ParserRuleReturnScope
	{
		CommonTree tree;

		@Override
		public CommonTree getTree() {
			return tree;
		}
	};

	// $ANTLR start "multiplicativeExpression"
	// ECalc.g:86:1: multiplicativeExpression : powerExpression ( ( MULT | DIV |
	// MOD ) ^ powerExpression )* ;
	public final BasicMathParser.multiplicativeExpression_return multiplicativeExpression()
			throws RecognitionException {
		final BasicMathParser.multiplicativeExpression_return retval = new BasicMathParser.multiplicativeExpression_return();
		retval.start = input.LT(1);

		CommonTree root_0 = null;

		Token set19 = null;
		ParserRuleReturnScope powerExpression18 = null;
		ParserRuleReturnScope powerExpression20 = null;

		final CommonTree set19_tree = null;

		try {
			// ECalc.g:87:2: ( powerExpression ( ( MULT | DIV | MOD ) ^
			// powerExpression )* )
			// ECalc.g:87:4: powerExpression ( ( MULT | DIV | MOD ) ^
			// powerExpression )*
			{
				root_0 = (CommonTree) adaptor.nil();

				pushFollow(FOLLOW_powerExpression_in_multiplicativeExpression299);
				powerExpression18 = powerExpression();
				state._fsp--;

				adaptor.addChild(
						root_0,
						powerExpression18.getTree());

				// ECalc.g:87:20: ( ( MULT | DIV | MOD ) ^ powerExpression )*
				loop6: while (true) {
					int alt6 = 2;
					final int LA6_0 = input.LA(1);
					if (((LA6_0 == DIV) || ((LA6_0 >= MOD) && (LA6_0 <= MULT)))) {
						alt6 = 1;
					}

					switch (alt6) {
						case 1:
						// ECalc.g:87:22: ( MULT | DIV | MOD ) ^ powerExpression
						{
							set19 = input.LT(1);
							set19 = input.LT(1);
							if ((input.LA(1) == DIV) || ((input.LA(1) >= MOD) && (input.LA(1) <= MULT))) {
								input.consume();
								root_0 = (CommonTree) adaptor.becomeRoot(
										adaptor.create(set19),
										root_0);
								state.errorRecovery = false;
							}
							else {
								final MismatchedSetException mse = new MismatchedSetException(
										null,
										input);
								throw mse;
							}
							pushFollow(FOLLOW_powerExpression_in_multiplicativeExpression312);
							powerExpression20 = powerExpression();
							state._fsp--;

							adaptor.addChild(
									root_0,
									powerExpression20.getTree());

						}
							break;

						default:
							break loop6;
					}
				}

			}

			retval.stop = input.LT(-1);

			retval.tree = (CommonTree) adaptor.rulePostProcessing(root_0);
			adaptor.setTokenBoundaries(
					retval.tree,
					retval.start,
					retval.stop);

		}
		catch (final RecognitionException re) {
			reportError(re);
			recover(
					input,
					re);
			retval.tree = (CommonTree) adaptor.errorNode(
					input,
					retval.start,
					input.LT(-1),
					re);
		}
		finally {
			// do for sure before leaving
		}
		return retval;
	}

	// $ANTLR end "multiplicativeExpression"

	public static class powerExpression_return extends
			ParserRuleReturnScope
	{
		CommonTree tree;

		@Override
		public CommonTree getTree() {
			return tree;
		}
	};

	// $ANTLR start "powerExpression"
	// ECalc.g:94:1: powerExpression : unaryExpression ( POW ^ unaryExpression
	// )* ;
	public final BasicMathParser.powerExpression_return powerExpression()
			throws RecognitionException {
		final BasicMathParser.powerExpression_return retval = new BasicMathParser.powerExpression_return();
		retval.start = input.LT(1);

		CommonTree root_0 = null;

		Token POW22 = null;
		ParserRuleReturnScope unaryExpression21 = null;
		ParserRuleReturnScope unaryExpression23 = null;

		CommonTree POW22_tree = null;

		try {
			// ECalc.g:95:2: ( unaryExpression ( POW ^ unaryExpression )* )
			// ECalc.g:95:4: unaryExpression ( POW ^ unaryExpression )*
			{
				root_0 = (CommonTree) adaptor.nil();

				pushFollow(FOLLOW_unaryExpression_in_powerExpression350);
				unaryExpression21 = unaryExpression();
				state._fsp--;

				adaptor.addChild(
						root_0,
						unaryExpression21.getTree());

				// ECalc.g:95:20: ( POW ^ unaryExpression )*
				loop7: while (true) {
					int alt7 = 2;
					final int LA7_0 = input.LA(1);
					if ((LA7_0 == POW)) {
						alt7 = 1;
					}

					switch (alt7) {
						case 1:
						// ECalc.g:95:22: POW ^ unaryExpression
						{
							POW22 = (Token) match(
									input,
									POW,
									FOLLOW_POW_in_powerExpression354);
							POW22_tree = (CommonTree) adaptor.create(POW22);
							root_0 = (CommonTree) adaptor.becomeRoot(
									POW22_tree,
									root_0);

							pushFollow(FOLLOW_unaryExpression_in_powerExpression357);
							unaryExpression23 = unaryExpression();
							state._fsp--;

							adaptor.addChild(
									root_0,
									unaryExpression23.getTree());

						}
							break;

						default:
							break loop7;
					}
				}

			}

			retval.stop = input.LT(-1);

			retval.tree = (CommonTree) adaptor.rulePostProcessing(root_0);
			adaptor.setTokenBoundaries(
					retval.tree,
					retval.start,
					retval.stop);

		}
		catch (final RecognitionException re) {
			reportError(re);
			recover(
					input,
					re);
			retval.tree = (CommonTree) adaptor.errorNode(
					input,
					retval.start,
					input.LT(-1),
					re);
		}
		finally {
			// do for sure before leaving
		}
		return retval;
	}

	// $ANTLR end "powerExpression"

	public static class unaryExpression_return extends
			ParserRuleReturnScope
	{
		CommonTree tree;

		@Override
		public CommonTree getTree() {
			return tree;
		}
	};

	// $ANTLR start "unaryExpression"
	// ECalc.g:100:1: unaryExpression : ( primaryExpression | NOT ^
	// primaryExpression | MINUS primaryExpression -> ^( NEGATE
	// primaryExpression ) );
	public final BasicMathParser.unaryExpression_return unaryExpression()
			throws RecognitionException {
		final BasicMathParser.unaryExpression_return retval = new BasicMathParser.unaryExpression_return();
		retval.start = input.LT(1);

		CommonTree root_0 = null;

		Token NOT25 = null;
		Token MINUS27 = null;
		ParserRuleReturnScope primaryExpression24 = null;
		ParserRuleReturnScope primaryExpression26 = null;
		ParserRuleReturnScope primaryExpression28 = null;

		CommonTree NOT25_tree = null;
		final CommonTree MINUS27_tree = null;
		final RewriteRuleTokenStream stream_MINUS = new RewriteRuleTokenStream(
				adaptor,
				"token MINUS");
		final RewriteRuleSubtreeStream stream_primaryExpression = new RewriteRuleSubtreeStream(
				adaptor,
				"rule primaryExpression");

		try {
			// ECalc.g:101:2: ( primaryExpression | NOT ^ primaryExpression |
			// MINUS primaryExpression -> ^( NEGATE primaryExpression ) )
			int alt8 = 3;
			switch (input.LA(1)) {
				case BOOLEAN:
				case DATETIME:
				case FLOAT:
				case IDENT:
				case INTEGER:
				case STRING:
				case 31:
				case 34: {
					alt8 = 1;
				}
					break;
				case NOT: {
					alt8 = 2;
				}
					break;
				case MINUS: {
					alt8 = 3;
				}
					break;
				default:
					final NoViableAltException nvae = new NoViableAltException(
							"",
							8,
							0,
							input);
					throw nvae;
			}
			switch (alt8) {
				case 1:
				// ECalc.g:101:4: primaryExpression
				{
					root_0 = (CommonTree) adaptor.nil();

					pushFollow(FOLLOW_primaryExpression_in_unaryExpression380);
					primaryExpression24 = primaryExpression();
					state._fsp--;

					adaptor.addChild(
							root_0,
							primaryExpression24.getTree());

				}
					break;
				case 2:
				// ECalc.g:102:8: NOT ^ primaryExpression
				{
					root_0 = (CommonTree) adaptor.nil();

					NOT25 = (Token) match(
							input,
							NOT,
							FOLLOW_NOT_in_unaryExpression389);
					NOT25_tree = (CommonTree) adaptor.create(NOT25);
					root_0 = (CommonTree) adaptor.becomeRoot(
							NOT25_tree,
							root_0);

					pushFollow(FOLLOW_primaryExpression_in_unaryExpression392);
					primaryExpression26 = primaryExpression();
					state._fsp--;

					adaptor.addChild(
							root_0,
							primaryExpression26.getTree());

				}
					break;
				case 3:
				// ECalc.g:103:8: MINUS primaryExpression
				{
					MINUS27 = (Token) match(
							input,
							MINUS,
							FOLLOW_MINUS_in_unaryExpression401);
					stream_MINUS.add(MINUS27);

					pushFollow(FOLLOW_primaryExpression_in_unaryExpression403);
					primaryExpression28 = primaryExpression();
					state._fsp--;

					stream_primaryExpression.add(primaryExpression28.getTree());
					// AST REWRITE
					// elements: primaryExpression
					// token labels:
					// rule labels: retval
					// token list labels:
					// rule list labels:
					// wildcard labels:
					retval.tree = root_0;
					final RewriteRuleSubtreeStream stream_retval = new RewriteRuleSubtreeStream(
							adaptor,
							"rule retval",
							retval != null ? retval.getTree() : null);

					root_0 = (CommonTree) adaptor.nil();
					// 103:32: -> ^( NEGATE primaryExpression )
					{
						// ECalc.g:103:35: ^( NEGATE primaryExpression )
						{
							CommonTree root_1 = (CommonTree) adaptor.nil();
							root_1 = (CommonTree) adaptor.becomeRoot(
									adaptor.create(
											NEGATE,
											"NEGATE"),
									root_1);
							adaptor.addChild(
									root_1,
									stream_primaryExpression.nextTree());
							adaptor.addChild(
									root_0,
									root_1);
						}

					}

					retval.tree = root_0;

				}
					break;

			}
			retval.stop = input.LT(-1);

			retval.tree = (CommonTree) adaptor.rulePostProcessing(root_0);
			adaptor.setTokenBoundaries(
					retval.tree,
					retval.start,
					retval.stop);

		}
		catch (final RecognitionException re) {
			reportError(re);
			recover(
					input,
					re);
			retval.tree = (CommonTree) adaptor.errorNode(
					input,
					retval.start,
					input.LT(-1),
					re);
		}
		finally {
			// do for sure before leaving
		}
		return retval;
	}

	// $ANTLR end "unaryExpression"

	public static class primaryExpression_return extends
			ParserRuleReturnScope
	{
		CommonTree tree;

		@Override
		public CommonTree getTree() {
			return tree;
		}
	};

	// $ANTLR start "primaryExpression"
	// ECalc.g:108:1: primaryExpression : ( '(' ! logicalExpression ')' !| value
	// );
	public final BasicMathParser.primaryExpression_return primaryExpression()
			throws RecognitionException {
		final BasicMathParser.primaryExpression_return retval = new BasicMathParser.primaryExpression_return();
		retval.start = input.LT(1);

		CommonTree root_0 = null;

		Token char_literal29 = null;
		Token char_literal31 = null;
		ParserRuleReturnScope logicalExpression30 = null;
		ParserRuleReturnScope value32 = null;

		final CommonTree char_literal29_tree = null;
		final CommonTree char_literal31_tree = null;

		try {
			// ECalc.g:109:2: ( '(' ! logicalExpression ')' !| value )
			int alt9 = 2;
			final int LA9_0 = input.LA(1);
			if ((LA9_0 == 31)) {
				alt9 = 1;
			}
			else if ((((LA9_0 >= BOOLEAN) && (LA9_0 <= DATETIME)) || (LA9_0 == FLOAT) || ((LA9_0 >= IDENT) && (LA9_0 <= INTEGER)) || (LA9_0 == STRING) || (LA9_0 == 34))) {
				alt9 = 2;
			}

			else {
				final NoViableAltException nvae = new NoViableAltException(
						"",
						9,
						0,
						input);
				throw nvae;
			}

			switch (alt9) {
				case 1:
				// ECalc.g:109:4: '(' ! logicalExpression ')' !
				{
					root_0 = (CommonTree) adaptor.nil();

					char_literal29 = (Token) match(
							input,
							31,
							FOLLOW_31_in_primaryExpression439);
					pushFollow(FOLLOW_logicalExpression_in_primaryExpression442);
					logicalExpression30 = logicalExpression();
					state._fsp--;

					adaptor.addChild(
							root_0,
							logicalExpression30.getTree());

					char_literal31 = (Token) match(
							input,
							32,
							FOLLOW_32_in_primaryExpression444);
				}
					break;
				case 2:
				// ECalc.g:110:4: value
				{
					root_0 = (CommonTree) adaptor.nil();

					pushFollow(FOLLOW_value_in_primaryExpression450);
					value32 = value();
					state._fsp--;

					adaptor.addChild(
							root_0,
							value32.getTree());

				}
					break;

			}
			retval.stop = input.LT(-1);

			retval.tree = (CommonTree) adaptor.rulePostProcessing(root_0);
			adaptor.setTokenBoundaries(
					retval.tree,
					retval.start,
					retval.stop);

		}
		catch (final RecognitionException re) {
			reportError(re);
			recover(
					input,
					re);
			retval.tree = (CommonTree) adaptor.errorNode(
					input,
					retval.start,
					input.LT(-1),
					re);
		}
		finally {
			// do for sure before leaving
		}
		return retval;
	}

	// $ANTLR end "primaryExpression"

	public static class value_return extends
			ParserRuleReturnScope
	{
		CommonTree tree;

		@Override
		public CommonTree getTree() {
			return tree;
		}
	};

	// $ANTLR start "value"
	// ECalc.g:113:1: value : ( INTEGER | FLOAT | DATETIME | BOOLEAN | STRING |
	// function | parameter );
	public final BasicMathParser.value_return value()
			throws RecognitionException {
		final BasicMathParser.value_return retval = new BasicMathParser.value_return();
		retval.start = input.LT(1);

		CommonTree root_0 = null;

		Token INTEGER33 = null;
		Token FLOAT34 = null;
		Token DATETIME35 = null;
		Token BOOLEAN36 = null;
		Token STRING37 = null;
		ParserRuleReturnScope function38 = null;
		ParserRuleReturnScope parameter39 = null;

		CommonTree INTEGER33_tree = null;
		CommonTree FLOAT34_tree = null;
		CommonTree DATETIME35_tree = null;
		CommonTree BOOLEAN36_tree = null;
		CommonTree STRING37_tree = null;

		try {
			// ECalc.g:114:2: ( INTEGER | FLOAT | DATETIME | BOOLEAN | STRING |
			// function | parameter )
			int alt10 = 7;
			switch (input.LA(1)) {
				case INTEGER: {
					alt10 = 1;
				}
					break;
				case FLOAT: {
					alt10 = 2;
				}
					break;
				case DATETIME: {
					alt10 = 3;
				}
					break;
				case BOOLEAN: {
					alt10 = 4;
				}
					break;
				case STRING: {
					alt10 = 5;
				}
					break;
				case IDENT: {
					alt10 = 6;
				}
					break;
				case 34: {
					alt10 = 7;
				}
					break;
				default:
					final NoViableAltException nvae = new NoViableAltException(
							"",
							10,
							0,
							input);
					throw nvae;
			}
			switch (alt10) {
				case 1:
				// ECalc.g:114:5: INTEGER
				{
					root_0 = (CommonTree) adaptor.nil();

					INTEGER33 = (Token) match(
							input,
							INTEGER,
							FOLLOW_INTEGER_in_value464);
					INTEGER33_tree = (CommonTree) adaptor.create(INTEGER33);
					adaptor.addChild(
							root_0,
							INTEGER33_tree);

				}
					break;
				case 2:
				// ECalc.g:115:4: FLOAT
				{
					root_0 = (CommonTree) adaptor.nil();

					FLOAT34 = (Token) match(
							input,
							FLOAT,
							FOLLOW_FLOAT_in_value469);
					FLOAT34_tree = (CommonTree) adaptor.create(FLOAT34);
					adaptor.addChild(
							root_0,
							FLOAT34_tree);

				}
					break;
				case 3:
				// ECalc.g:116:5: DATETIME
				{
					root_0 = (CommonTree) adaptor.nil();

					DATETIME35 = (Token) match(
							input,
							DATETIME,
							FOLLOW_DATETIME_in_value475);
					DATETIME35_tree = (CommonTree) adaptor.create(DATETIME35);
					adaptor.addChild(
							root_0,
							DATETIME35_tree);

				}
					break;
				case 4:
				// ECalc.g:117:4: BOOLEAN
				{
					root_0 = (CommonTree) adaptor.nil();

					BOOLEAN36 = (Token) match(
							input,
							BOOLEAN,
							FOLLOW_BOOLEAN_in_value480);
					BOOLEAN36_tree = (CommonTree) adaptor.create(BOOLEAN36);
					adaptor.addChild(
							root_0,
							BOOLEAN36_tree);

				}
					break;
				case 5:
				// ECalc.g:118:4: STRING
				{
					root_0 = (CommonTree) adaptor.nil();

					STRING37 = (Token) match(
							input,
							STRING,
							FOLLOW_STRING_in_value485);
					STRING37_tree = (CommonTree) adaptor.create(STRING37);
					adaptor.addChild(
							root_0,
							STRING37_tree);

				}
					break;
				case 6:
				// ECalc.g:119:4: function
				{
					root_0 = (CommonTree) adaptor.nil();

					pushFollow(FOLLOW_function_in_value490);
					function38 = function();
					state._fsp--;

					adaptor.addChild(
							root_0,
							function38.getTree());

				}
					break;
				case 7:
				// ECalc.g:120:4: parameter
				{
					root_0 = (CommonTree) adaptor.nil();

					pushFollow(FOLLOW_parameter_in_value495);
					parameter39 = parameter();
					state._fsp--;

					adaptor.addChild(
							root_0,
							parameter39.getTree());

				}
					break;

			}
			retval.stop = input.LT(-1);

			retval.tree = (CommonTree) adaptor.rulePostProcessing(root_0);
			adaptor.setTokenBoundaries(
					retval.tree,
					retval.start,
					retval.stop);

		}
		catch (final RecognitionException re) {
			reportError(re);
			recover(
					input,
					re);
			retval.tree = (CommonTree) adaptor.errorNode(
					input,
					retval.start,
					input.LT(-1),
					re);
		}
		finally {
			// do for sure before leaving
		}
		return retval;
	}

	// $ANTLR end "value"

	public static class parameter_return extends
			ParserRuleReturnScope
	{
		CommonTree tree;

		@Override
		public CommonTree getTree() {
			return tree;
		}
	};

	// $ANTLR start "parameter"
	// ECalc.g:123:1: parameter : '[' ( IDENT | INTEGER ) ']' -> ^( PARAM (
	// IDENT )? ( INTEGER )? ) ;
	public final BasicMathParser.parameter_return parameter()
			throws RecognitionException {
		final BasicMathParser.parameter_return retval = new BasicMathParser.parameter_return();
		retval.start = input.LT(1);

		CommonTree root_0 = null;

		Token char_literal40 = null;
		Token IDENT41 = null;
		Token INTEGER42 = null;
		Token char_literal43 = null;

		final CommonTree char_literal40_tree = null;
		final CommonTree IDENT41_tree = null;
		final CommonTree INTEGER42_tree = null;
		final CommonTree char_literal43_tree = null;
		final RewriteRuleTokenStream stream_INTEGER = new RewriteRuleTokenStream(
				adaptor,
				"token INTEGER");
		final RewriteRuleTokenStream stream_IDENT = new RewriteRuleTokenStream(
				adaptor,
				"token IDENT");
		final RewriteRuleTokenStream stream_35 = new RewriteRuleTokenStream(
				adaptor,
				"token 35");
		final RewriteRuleTokenStream stream_34 = new RewriteRuleTokenStream(
				adaptor,
				"token 34");

		try {
			// ECalc.g:124:2: ( '[' ( IDENT | INTEGER ) ']' -> ^( PARAM ( IDENT
			// )? ( INTEGER )? ) )
			// ECalc.g:124:4: '[' ( IDENT | INTEGER ) ']'
			{
				char_literal40 = (Token) match(
						input,
						34,
						FOLLOW_34_in_parameter506);
				stream_34.add(char_literal40);

				// ECalc.g:124:8: ( IDENT | INTEGER )
				int alt11 = 2;
				final int LA11_0 = input.LA(1);
				if ((LA11_0 == IDENT)) {
					alt11 = 1;
				}
				else if ((LA11_0 == INTEGER)) {
					alt11 = 2;
				}

				else {
					final NoViableAltException nvae = new NoViableAltException(
							"",
							11,
							0,
							input);
					throw nvae;
				}

				switch (alt11) {
					case 1:
					// ECalc.g:124:9: IDENT
					{
						IDENT41 = (Token) match(
								input,
								IDENT,
								FOLLOW_IDENT_in_parameter509);
						stream_IDENT.add(IDENT41);

					}
						break;
					case 2:
					// ECalc.g:124:15: INTEGER
					{
						INTEGER42 = (Token) match(
								input,
								INTEGER,
								FOLLOW_INTEGER_in_parameter511);
						stream_INTEGER.add(INTEGER42);

					}
						break;

				}

				char_literal43 = (Token) match(
						input,
						35,
						FOLLOW_35_in_parameter514);
				stream_35.add(char_literal43);

				// AST REWRITE
				// elements: IDENT, INTEGER
				// token labels:
				// rule labels: retval
				// token list labels:
				// rule list labels:
				// wildcard labels:
				retval.tree = root_0;
				final RewriteRuleSubtreeStream stream_retval = new RewriteRuleSubtreeStream(
						adaptor,
						"rule retval",
						retval != null ? retval.getTree() : null);

				root_0 = (CommonTree) adaptor.nil();
				// 124:28: -> ^( PARAM ( IDENT )? ( INTEGER )? )
				{
					// ECalc.g:124:31: ^( PARAM ( IDENT )? ( INTEGER )? )
					{
						CommonTree root_1 = (CommonTree) adaptor.nil();
						root_1 = (CommonTree) adaptor.becomeRoot(
								adaptor.create(
										PARAM,
										"PARAM"),
								root_1);
						// ECalc.g:124:39: ( IDENT )?
						if (stream_IDENT.hasNext()) {
							adaptor.addChild(
									root_1,
									stream_IDENT.nextNode());
						}
						stream_IDENT.reset();

						// ECalc.g:124:46: ( INTEGER )?
						if (stream_INTEGER.hasNext()) {
							adaptor.addChild(
									root_1,
									stream_INTEGER.nextNode());
						}
						stream_INTEGER.reset();

						adaptor.addChild(
								root_0,
								root_1);
					}

				}

				retval.tree = root_0;

			}

			retval.stop = input.LT(-1);

			retval.tree = (CommonTree) adaptor.rulePostProcessing(root_0);
			adaptor.setTokenBoundaries(
					retval.tree,
					retval.start,
					retval.stop);

		}
		catch (final RecognitionException re) {
			reportError(re);
			recover(
					input,
					re);
			retval.tree = (CommonTree) adaptor.errorNode(
					input,
					retval.start,
					input.LT(-1),
					re);
		}
		finally {
			// do for sure before leaving
		}
		return retval;
	}

	// $ANTLR end "parameter"

	public static class function_return extends
			ParserRuleReturnScope
	{
		CommonTree tree;

		@Override
		public CommonTree getTree() {
			return tree;
		}
	};

	// $ANTLR start "function"
	// ECalc.g:149:1: function : IDENT '(' ( logicalExpression ( ','
	// logicalExpression )* )? ')' -> ^( IDENT ( logicalExpression )* ) ;
	public final BasicMathParser.function_return function()
			throws RecognitionException {
		final BasicMathParser.function_return retval = new BasicMathParser.function_return();
		retval.start = input.LT(1);

		CommonTree root_0 = null;

		Token IDENT44 = null;
		Token char_literal45 = null;
		Token char_literal47 = null;
		Token char_literal49 = null;
		ParserRuleReturnScope logicalExpression46 = null;
		ParserRuleReturnScope logicalExpression48 = null;

		final CommonTree IDENT44_tree = null;
		final CommonTree char_literal45_tree = null;
		final CommonTree char_literal47_tree = null;
		final CommonTree char_literal49_tree = null;
		final RewriteRuleTokenStream stream_IDENT = new RewriteRuleTokenStream(
				adaptor,
				"token IDENT");
		final RewriteRuleTokenStream stream_32 = new RewriteRuleTokenStream(
				adaptor,
				"token 32");
		final RewriteRuleTokenStream stream_31 = new RewriteRuleTokenStream(
				adaptor,
				"token 31");
		final RewriteRuleTokenStream stream_33 = new RewriteRuleTokenStream(
				adaptor,
				"token 33");
		final RewriteRuleSubtreeStream stream_logicalExpression = new RewriteRuleSubtreeStream(
				adaptor,
				"rule logicalExpression");

		try {
			// ECalc.g:150:2: ( IDENT '(' ( logicalExpression ( ','
			// logicalExpression )* )? ')' -> ^( IDENT ( logicalExpression )* )
			// )
			// ECalc.g:150:4: IDENT '(' ( logicalExpression ( ','
			// logicalExpression )* )? ')'
			{
				IDENT44 = (Token) match(
						input,
						IDENT,
						FOLLOW_IDENT_in_function685);
				stream_IDENT.add(IDENT44);

				char_literal45 = (Token) match(
						input,
						31,
						FOLLOW_31_in_function687);
				stream_31.add(char_literal45);

				// ECalc.g:150:14: ( logicalExpression ( ',' logicalExpression
				// )* )?
				int alt13 = 2;
				final int LA13_0 = input.LA(1);
				if ((((LA13_0 >= BOOLEAN) && (LA13_0 <= DATETIME)) || (LA13_0 == FLOAT) || ((LA13_0 >= IDENT) && (LA13_0 <= INTEGER)) || (LA13_0 == MINUS) || (LA13_0 == NOT) || (LA13_0 == STRING) || (LA13_0 == 31) || (LA13_0 == 34))) {
					alt13 = 1;
				}
				switch (alt13) {
					case 1:
					// ECalc.g:150:16: logicalExpression ( ',' logicalExpression
					// )*
					{
						pushFollow(FOLLOW_logicalExpression_in_function691);
						logicalExpression46 = logicalExpression();
						state._fsp--;

						stream_logicalExpression.add(logicalExpression46.getTree());
						// ECalc.g:150:34: ( ',' logicalExpression )*
						loop12: while (true) {
							int alt12 = 2;
							final int LA12_0 = input.LA(1);
							if ((LA12_0 == 33)) {
								alt12 = 1;
							}

							switch (alt12) {
								case 1:
								// ECalc.g:150:35: ',' logicalExpression
								{
									char_literal47 = (Token) match(
											input,
											33,
											FOLLOW_33_in_function694);
									stream_33.add(char_literal47);

									pushFollow(FOLLOW_logicalExpression_in_function696);
									logicalExpression48 = logicalExpression();
									state._fsp--;

									stream_logicalExpression.add(logicalExpression48.getTree());
								}
									break;

								default:
									break loop12;
							}
						}

					}
						break;

				}

				char_literal49 = (Token) match(
						input,
						32,
						FOLLOW_32_in_function703);
				stream_32.add(char_literal49);

				// AST REWRITE
				// elements: IDENT, logicalExpression
				// token labels:
				// rule labels: retval
				// token list labels:
				// rule list labels:
				// wildcard labels:
				retval.tree = root_0;
				final RewriteRuleSubtreeStream stream_retval = new RewriteRuleSubtreeStream(
						adaptor,
						"rule retval",
						retval != null ? retval.getTree() : null);

				root_0 = (CommonTree) adaptor.nil();
				// 150:66: -> ^( IDENT ( logicalExpression )* )
				{
					// ECalc.g:150:69: ^( IDENT ( logicalExpression )* )
					{
						CommonTree root_1 = (CommonTree) adaptor.nil();
						root_1 = (CommonTree) adaptor.becomeRoot(
								stream_IDENT.nextNode(),
								root_1);
						// ECalc.g:150:77: ( logicalExpression )*
						while (stream_logicalExpression.hasNext()) {
							adaptor.addChild(
									root_1,
									stream_logicalExpression.nextTree());
						}
						stream_logicalExpression.reset();

						adaptor.addChild(
								root_0,
								root_1);
					}

				}

				retval.tree = root_0;

			}

			retval.stop = input.LT(-1);

			retval.tree = (CommonTree) adaptor.rulePostProcessing(root_0);
			adaptor.setTokenBoundaries(
					retval.tree,
					retval.start,
					retval.stop);

		}
		catch (final RecognitionException re) {
			reportError(re);
			recover(
					input,
					re);
			retval.tree = (CommonTree) adaptor.errorNode(
					input,
					retval.start,
					input.LT(-1),
					re);
		}
		finally {
			// do for sure before leaving
		}
		return retval;
	}

	// $ANTLR end "function"

	// Delegated rules

	public static final BitSet FOLLOW_logicalExpression_in_expression64 = new BitSet(
			new long[] {
				0x0000000000000000L
			});
	public static final BitSet FOLLOW_EOF_in_expression66 = new BitSet(
			new long[] {
				0x0000000000000002L
			});
	public static final BitSet FOLLOW_booleanAndExpression_in_logicalExpression80 = new BitSet(
			new long[] {
				0x0000000001000002L
			});
	public static final BitSet FOLLOW_OR_in_logicalExpression83 = new BitSet(
			new long[] {
				0x000000049044C460L
			});
	public static final BitSet FOLLOW_booleanAndExpression_in_logicalExpression86 = new BitSet(
			new long[] {
				0x0000000001000002L
			});
	public static final BitSet FOLLOW_equalityExpression_in_booleanAndExpression116 = new BitSet(
			new long[] {
				0x0000000000000012L
			});
	public static final BitSet FOLLOW_AND_in_booleanAndExpression119 = new BitSet(
			new long[] {
				0x000000049044C460L
			});
	public static final BitSet FOLLOW_equalityExpression_in_booleanAndExpression122 = new BitSet(
			new long[] {
				0x0000000000000012L
			});
	public static final BitSet FOLLOW_relationalExpression_in_equalityExpression150 = new BitSet(
			new long[] {
				0x0000000000800102L
			});
	public static final BitSet FOLLOW_set_in_equalityExpression153 = new BitSet(
			new long[] {
				0x000000049044C460L
			});
	public static final BitSet FOLLOW_relationalExpression_in_equalityExpression160 = new BitSet(
			new long[] {
				0x0000000000800102L
			});
	public static final BitSet FOLLOW_additiveExpression_in_relationalExpression200 = new BitSet(
			new long[] {
				0x0000000000031802L
			});
	public static final BitSet FOLLOW_set_in_relationalExpression204 = new BitSet(
			new long[] {
				0x000000049044C460L
			});
	public static final BitSet FOLLOW_additiveExpression_in_relationalExpression215 = new BitSet(
			new long[] {
				0x0000000000031802L
			});
	public static final BitSet FOLLOW_multiplicativeExpression_in_additiveExpression258 = new BitSet(
			new long[] {
				0x0000000004040002L
			});
	public static final BitSet FOLLOW_set_in_additiveExpression262 = new BitSet(
			new long[] {
				0x000000049044C460L
			});
	public static final BitSet FOLLOW_multiplicativeExpression_in_additiveExpression269 = new BitSet(
			new long[] {
				0x0000000004040002L
			});
	public static final BitSet FOLLOW_powerExpression_in_multiplicativeExpression299 = new BitSet(
			new long[] {
				0x0000000000180082L
			});
	public static final BitSet FOLLOW_set_in_multiplicativeExpression303 = new BitSet(
			new long[] {
				0x000000049044C460L
			});
	public static final BitSet FOLLOW_powerExpression_in_multiplicativeExpression312 = new BitSet(
			new long[] {
				0x0000000000180082L
			});
	public static final BitSet FOLLOW_unaryExpression_in_powerExpression350 = new BitSet(
			new long[] {
				0x0000000008000002L
			});
	public static final BitSet FOLLOW_POW_in_powerExpression354 = new BitSet(
			new long[] {
				0x000000049044C460L
			});
	public static final BitSet FOLLOW_unaryExpression_in_powerExpression357 = new BitSet(
			new long[] {
				0x0000000008000002L
			});
	public static final BitSet FOLLOW_primaryExpression_in_unaryExpression380 = new BitSet(
			new long[] {
				0x0000000000000002L
			});
	public static final BitSet FOLLOW_NOT_in_unaryExpression389 = new BitSet(
			new long[] {
				0x000000049000C460L
			});
	public static final BitSet FOLLOW_primaryExpression_in_unaryExpression392 = new BitSet(
			new long[] {
				0x0000000000000002L
			});
	public static final BitSet FOLLOW_MINUS_in_unaryExpression401 = new BitSet(
			new long[] {
				0x000000049000C460L
			});
	public static final BitSet FOLLOW_primaryExpression_in_unaryExpression403 = new BitSet(
			new long[] {
				0x0000000000000002L
			});
	public static final BitSet FOLLOW_31_in_primaryExpression439 = new BitSet(
			new long[] {
				0x000000049044C460L
			});
	public static final BitSet FOLLOW_logicalExpression_in_primaryExpression442 = new BitSet(
			new long[] {
				0x0000000100000000L
			});
	public static final BitSet FOLLOW_32_in_primaryExpression444 = new BitSet(
			new long[] {
				0x0000000000000002L
			});
	public static final BitSet FOLLOW_value_in_primaryExpression450 = new BitSet(
			new long[] {
				0x0000000000000002L
			});
	public static final BitSet FOLLOW_INTEGER_in_value464 = new BitSet(
			new long[] {
				0x0000000000000002L
			});
	public static final BitSet FOLLOW_FLOAT_in_value469 = new BitSet(
			new long[] {
				0x0000000000000002L
			});
	public static final BitSet FOLLOW_DATETIME_in_value475 = new BitSet(
			new long[] {
				0x0000000000000002L
			});
	public static final BitSet FOLLOW_BOOLEAN_in_value480 = new BitSet(
			new long[] {
				0x0000000000000002L
			});
	public static final BitSet FOLLOW_STRING_in_value485 = new BitSet(
			new long[] {
				0x0000000000000002L
			});
	public static final BitSet FOLLOW_function_in_value490 = new BitSet(
			new long[] {
				0x0000000000000002L
			});
	public static final BitSet FOLLOW_parameter_in_value495 = new BitSet(
			new long[] {
				0x0000000000000002L
			});
	public static final BitSet FOLLOW_34_in_parameter506 = new BitSet(
			new long[] {
				0x000000000000C000L
			});
	public static final BitSet FOLLOW_IDENT_in_parameter509 = new BitSet(
			new long[] {
				0x0000000800000000L
			});
	public static final BitSet FOLLOW_INTEGER_in_parameter511 = new BitSet(
			new long[] {
				0x0000000800000000L
			});
	public static final BitSet FOLLOW_35_in_parameter514 = new BitSet(
			new long[] {
				0x0000000000000002L
			});
	public static final BitSet FOLLOW_IDENT_in_function685 = new BitSet(
			new long[] {
				0x0000000080000000L
			});
	public static final BitSet FOLLOW_31_in_function687 = new BitSet(
			new long[] {
				0x000000059044C460L
			});
	public static final BitSet FOLLOW_logicalExpression_in_function691 = new BitSet(
			new long[] {
				0x0000000300000000L
			});
	public static final BitSet FOLLOW_33_in_function694 = new BitSet(
			new long[] {
				0x000000049044C460L
			});
	public static final BitSet FOLLOW_logicalExpression_in_function696 = new BitSet(
			new long[] {
				0x0000000300000000L
			});
	public static final BitSet FOLLOW_32_in_function703 = new BitSet(
			new long[] {
				0x0000000000000002L
			});
}
