package mil.nga.giat.geowave.analytics.mapreduce.kde.parser;

// $ANTLR 3.3 BasicMath.g 2014-05-13 05:58:45

import org.antlr.runtime.*;
import java.util.Stack;
import java.util.List;
import java.util.ArrayList;

@SuppressWarnings("all")
public class BasicMathLexer extends
		Lexer
{
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
	// delegators
	public Lexer[] getDelegates() {
		return new Lexer[] {};
	}

	public BasicMathLexer() {}

	public BasicMathLexer(
			CharStream input ) {
		this(
				input,
				new RecognizerSharedState());
	}

	public BasicMathLexer(
			CharStream input,
			RecognizerSharedState state ) {
		super(
				input,
				state);
	}

	@Override
	public String getGrammarFileName() {
		return "ECalc.g";
	}

	// $ANTLR start "T__31"
	public final void mT__31()
			throws RecognitionException {
		try {
			int _type = T__31;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// ECalc.g:7:7: ( '(' )
			// ECalc.g:7:9: '('
			{
				match('(');
			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}

	// $ANTLR end "T__31"

	// $ANTLR start "T__32"
	public final void mT__32()
			throws RecognitionException {
		try {
			int _type = T__32;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// ECalc.g:8:7: ( ')' )
			// ECalc.g:8:9: ')'
			{
				match(')');
			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}

	// $ANTLR end "T__32"

	// $ANTLR start "T__33"
	public final void mT__33()
			throws RecognitionException {
		try {
			int _type = T__33;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// ECalc.g:9:7: ( ',' )
			// ECalc.g:9:9: ','
			{
				match(',');
			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}

	// $ANTLR end "T__33"

	// $ANTLR start "T__34"
	public final void mT__34()
			throws RecognitionException {
		try {
			int _type = T__34;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// ECalc.g:10:7: ( '[' )
			// ECalc.g:10:9: '['
			{
				match('[');
			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}

	// $ANTLR end "T__34"

	// $ANTLR start "T__35"
	public final void mT__35()
			throws RecognitionException {
		try {
			int _type = T__35;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// ECalc.g:11:7: ( ']' )
			// ECalc.g:11:9: ']'
			{
				match(']');
			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}

	// $ANTLR end "T__35"

	// $ANTLR start "OR"
	public final void mOR()
			throws RecognitionException {
		try {
			int _type = OR;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// ECalc.g:53:5: ( '||' | 'or' )
			int alt1 = 2;
			int LA1_0 = input.LA(1);
			if ((LA1_0 == '|')) {
				alt1 = 1;
			}
			else if ((LA1_0 == 'o')) {
				alt1 = 2;
			}

			else {
				NoViableAltException nvae = new NoViableAltException(
						"",
						1,
						0,
						input);
				throw nvae;
			}

			switch (alt1) {
				case 1:
				// ECalc.g:53:8: '||'
				{
					match("||");

				}
					break;
				case 2:
				// ECalc.g:53:15: 'or'
				{
					match("or");

				}
					break;

			}
			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}

	// $ANTLR end "OR"

	// $ANTLR start "AND"
	public final void mAND()
			throws RecognitionException {
		try {
			int _type = AND;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// ECalc.g:59:6: ( '&&' | 'and' )
			int alt2 = 2;
			int LA2_0 = input.LA(1);
			if ((LA2_0 == '&')) {
				alt2 = 1;
			}
			else if ((LA2_0 == 'a')) {
				alt2 = 2;
			}

			else {
				NoViableAltException nvae = new NoViableAltException(
						"",
						2,
						0,
						input);
				throw nvae;
			}

			switch (alt2) {
				case 1:
				// ECalc.g:59:9: '&&'
				{
					match("&&");

				}
					break;
				case 2:
				// ECalc.g:59:16: 'and'
				{
					match("and");

				}
					break;

			}
			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}

	// $ANTLR end "AND"

	// $ANTLR start "EQUALS"
	public final void mEQUALS()
			throws RecognitionException {
		try {
			int _type = EQUALS;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// ECalc.g:66:2: ( '=' | '==' )
			int alt3 = 2;
			int LA3_0 = input.LA(1);
			if ((LA3_0 == '=')) {
				int LA3_1 = input.LA(2);
				if ((LA3_1 == '=')) {
					alt3 = 2;
				}

				else {
					alt3 = 1;
				}

			}

			else {
				NoViableAltException nvae = new NoViableAltException(
						"",
						3,
						0,
						input);
				throw nvae;
			}

			switch (alt3) {
				case 1:
				// ECalc.g:66:4: '='
				{
					match('=');
				}
					break;
				case 2:
				// ECalc.g:66:10: '=='
				{
					match("==");

				}
					break;

			}
			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}

	// $ANTLR end "EQUALS"

	// $ANTLR start "NOTEQUALS"
	public final void mNOTEQUALS()
			throws RecognitionException {
		try {
			int _type = NOTEQUALS;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// ECalc.g:68:2: ( '!=' | '<>' )
			int alt4 = 2;
			int LA4_0 = input.LA(1);
			if ((LA4_0 == '!')) {
				alt4 = 1;
			}
			else if ((LA4_0 == '<')) {
				alt4 = 2;
			}

			else {
				NoViableAltException nvae = new NoViableAltException(
						"",
						4,
						0,
						input);
				throw nvae;
			}

			switch (alt4) {
				case 1:
				// ECalc.g:68:4: '!='
				{
					match("!=");

				}
					break;
				case 2:
				// ECalc.g:68:11: '<>'
				{
					match("<>");

				}
					break;

			}
			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}

	// $ANTLR end "NOTEQUALS"

	// $ANTLR start "LT"
	public final void mLT()
			throws RecognitionException {
		try {
			int _type = LT;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// ECalc.g:74:4: ( '<' )
			// ECalc.g:74:6: '<'
			{
				match('<');
			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}

	// $ANTLR end "LT"

	// $ANTLR start "LTEQ"
	public final void mLTEQ()
			throws RecognitionException {
		try {
			int _type = LTEQ;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// ECalc.g:75:6: ( '<=' )
			// ECalc.g:75:8: '<='
			{
				match("<=");

			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}

	// $ANTLR end "LTEQ"

	// $ANTLR start "GT"
	public final void mGT()
			throws RecognitionException {
		try {
			int _type = GT;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// ECalc.g:76:4: ( '>' )
			// ECalc.g:76:6: '>'
			{
				match('>');
			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}

	// $ANTLR end "GT"

	// $ANTLR start "GTEQ"
	public final void mGTEQ()
			throws RecognitionException {
		try {
			int _type = GTEQ;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// ECalc.g:77:6: ( '>=' )
			// ECalc.g:77:8: '>='
			{
				match(">=");

			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}

	// $ANTLR end "GTEQ"

	// $ANTLR start "PLUS"
	public final void mPLUS()
			throws RecognitionException {
		try {
			int _type = PLUS;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// ECalc.g:83:6: ( '+' )
			// ECalc.g:83:8: '+'
			{
				match('+');
			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}

	// $ANTLR end "PLUS"

	// $ANTLR start "MINUS"
	public final void mMINUS()
			throws RecognitionException {
		try {
			int _type = MINUS;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// ECalc.g:84:7: ( '-' )
			// ECalc.g:84:9: '-'
			{
				match('-');
			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}

	// $ANTLR end "MINUS"

	// $ANTLR start "MULT"
	public final void mMULT()
			throws RecognitionException {
		try {
			int _type = MULT;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// ECalc.g:90:6: ( '*' )
			// ECalc.g:90:8: '*'
			{
				match('*');
			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}

	// $ANTLR end "MULT"

	// $ANTLR start "DIV"
	public final void mDIV()
			throws RecognitionException {
		try {
			int _type = DIV;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// ECalc.g:91:5: ( '/' )
			// ECalc.g:91:7: '/'
			{
				match('/');
			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}

	// $ANTLR end "DIV"

	// $ANTLR start "MOD"
	public final void mMOD()
			throws RecognitionException {
		try {
			int _type = MOD;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// ECalc.g:92:5: ( '%' )
			// ECalc.g:92:7: '%'
			{
				match('%');
			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}

	// $ANTLR end "MOD"

	// $ANTLR start "POW"
	public final void mPOW()
			throws RecognitionException {
		try {
			int _type = POW;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// ECalc.g:98:5: ( '^' )
			// ECalc.g:98:7: '^'
			{
				match('^');
			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}

	// $ANTLR end "POW"

	// $ANTLR start "NOT"
	public final void mNOT()
			throws RecognitionException {
		try {
			int _type = NOT;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// ECalc.g:106:5: ( '!' | 'not' )
			int alt5 = 2;
			int LA5_0 = input.LA(1);
			if ((LA5_0 == '!')) {
				alt5 = 1;
			}
			else if ((LA5_0 == 'n')) {
				alt5 = 2;
			}

			else {
				NoViableAltException nvae = new NoViableAltException(
						"",
						5,
						0,
						input);
				throw nvae;
			}

			switch (alt5) {
				case 1:
				// ECalc.g:106:7: '!'
				{
					match('!');
				}
					break;
				case 2:
				// ECalc.g:106:13: 'not'
				{
					match("not");

				}
					break;

			}
			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}

	// $ANTLR end "NOT"

	// $ANTLR start "STRING"
	public final void mSTRING()
			throws RecognitionException {
		try {
			int _type = STRING;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// ECalc.g:128:6: ( '\\'' ( EscapeSequence | ( options
			// {greedy=false; } :~ ( '\\u0000' .. '\\u001f' | '\\\\' | '\\'' ) )
			// )* '\\'' )
			// ECalc.g:128:10: '\\'' ( EscapeSequence | ( options {greedy=false;
			// } :~ ( '\\u0000' .. '\\u001f' | '\\\\' | '\\'' ) ) )* '\\''
			{
				match('\'');
				// ECalc.g:128:15: ( EscapeSequence | ( options {greedy=false; }
				// :~ ( '\\u0000' .. '\\u001f' | '\\\\' | '\\'' ) ) )*
				loop6: while (true) {
					int alt6 = 3;
					int LA6_0 = input.LA(1);
					if ((LA6_0 == '\\')) {
						alt6 = 1;
					}
					else if (((LA6_0 >= ' ' && LA6_0 <= '&') || (LA6_0 >= '(' && LA6_0 <= '[') || (LA6_0 >= ']' && LA6_0 <= '\uFFFF'))) {
						alt6 = 2;
					}

					switch (alt6) {
						case 1:
						// ECalc.g:128:17: EscapeSequence
						{
							mEscapeSequence();

						}
							break;
						case 2:
						// ECalc.g:128:34: ( options {greedy=false; } :~ (
						// '\\u0000' .. '\\u001f' | '\\\\' | '\\'' ) )
						{
							// ECalc.g:128:34: ( options {greedy=false; } :~ (
							// '\\u0000' .. '\\u001f' | '\\\\' | '\\'' ) )
							// ECalc.g:128:61: ~ ( '\\u0000' .. '\\u001f' |
							// '\\\\' | '\\'' )
							{
								if ((input.LA(1) >= ' ' && input.LA(1) <= '&') || (input.LA(1) >= '(' && input.LA(1) <= '[') || (input.LA(1) >= ']' && input.LA(1) <= '\uFFFF')) {
									input.consume();
								}
								else {
									MismatchedSetException mse = new MismatchedSetException(
											null,
											input);
									recover(mse);
									throw mse;
								}
							}

						}
							break;

						default:
							break loop6;
					}
				}

				match('\'');
			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}

	// $ANTLR end "STRING"

	// $ANTLR start "INTEGER"
	public final void mINTEGER()
			throws RecognitionException {
		try {
			int _type = INTEGER;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// ECalc.g:132:2: ( ( '0' .. '9' )+ )
			// ECalc.g:132:4: ( '0' .. '9' )+
			{
				// ECalc.g:132:4: ( '0' .. '9' )+
				int cnt7 = 0;
				loop7: while (true) {
					int alt7 = 2;
					int LA7_0 = input.LA(1);
					if (((LA7_0 >= '0' && LA7_0 <= '9'))) {
						alt7 = 1;
					}

					switch (alt7) {
						case 1:
						// ECalc.g:
						{
							if ((input.LA(1) >= '0' && input.LA(1) <= '9')) {
								input.consume();
							}
							else {
								MismatchedSetException mse = new MismatchedSetException(
										null,
										input);
								recover(mse);
								throw mse;
							}
						}
							break;

						default:
							if (cnt7 >= 1) break loop7;
							EarlyExitException eee = new EarlyExitException(
									7,
									input);
							throw eee;
					}
					cnt7++;
				}

			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}

	// $ANTLR end "INTEGER"

	// $ANTLR start "FLOAT"
	public final void mFLOAT()
			throws RecognitionException {
		try {
			int _type = FLOAT;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// ECalc.g:136:2: ( ( '0' .. '9' )* '.' ( '0' .. '9' )+ )
			// ECalc.g:136:4: ( '0' .. '9' )* '.' ( '0' .. '9' )+
			{
				// ECalc.g:136:4: ( '0' .. '9' )*
				loop8: while (true) {
					int alt8 = 2;
					int LA8_0 = input.LA(1);
					if (((LA8_0 >= '0' && LA8_0 <= '9'))) {
						alt8 = 1;
					}

					switch (alt8) {
						case 1:
						// ECalc.g:
						{
							if ((input.LA(1) >= '0' && input.LA(1) <= '9')) {
								input.consume();
							}
							else {
								MismatchedSetException mse = new MismatchedSetException(
										null,
										input);
								recover(mse);
								throw mse;
							}
						}
							break;

						default:
							break loop8;
					}
				}

				match('.');
				// ECalc.g:136:20: ( '0' .. '9' )+
				int cnt9 = 0;
				loop9: while (true) {
					int alt9 = 2;
					int LA9_0 = input.LA(1);
					if (((LA9_0 >= '0' && LA9_0 <= '9'))) {
						alt9 = 1;
					}

					switch (alt9) {
						case 1:
						// ECalc.g:
						{
							if ((input.LA(1) >= '0' && input.LA(1) <= '9')) {
								input.consume();
							}
							else {
								MismatchedSetException mse = new MismatchedSetException(
										null,
										input);
								recover(mse);
								throw mse;
							}
						}
							break;

						default:
							if (cnt9 >= 1) break loop9;
							EarlyExitException eee = new EarlyExitException(
									9,
									input);
							throw eee;
					}
					cnt9++;
				}

			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}

	// $ANTLR end "FLOAT"

	// $ANTLR start "DATETIME"
	public final void mDATETIME()
			throws RecognitionException {
		try {
			int _type = DATETIME;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// ECalc.g:140:3: ( '#' (~ '#' )* '#' )
			// ECalc.g:140:5: '#' (~ '#' )* '#'
			{
				match('#');
				// ECalc.g:140:9: (~ '#' )*
				loop10: while (true) {
					int alt10 = 2;
					int LA10_0 = input.LA(1);
					if (((LA10_0 >= '\u0000' && LA10_0 <= '\"') || (LA10_0 >= '$' && LA10_0 <= '\uFFFF'))) {
						alt10 = 1;
					}

					switch (alt10) {
						case 1:
						// ECalc.g:
						{
							if ((input.LA(1) >= '\u0000' && input.LA(1) <= '\"') || (input.LA(1) >= '$' && input.LA(1) <= '\uFFFF')) {
								input.consume();
							}
							else {
								MismatchedSetException mse = new MismatchedSetException(
										null,
										input);
								recover(mse);
								throw mse;
							}
						}
							break;

						default:
							break loop10;
					}
				}

				match('#');
			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}

	// $ANTLR end "DATETIME"

	// $ANTLR start "BOOLEAN"
	public final void mBOOLEAN()
			throws RecognitionException {
		try {
			int _type = BOOLEAN;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// ECalc.g:144:2: ( 'true' | 'false' )
			int alt11 = 2;
			int LA11_0 = input.LA(1);
			if ((LA11_0 == 't')) {
				alt11 = 1;
			}
			else if ((LA11_0 == 'f')) {
				alt11 = 2;
			}

			else {
				NoViableAltException nvae = new NoViableAltException(
						"",
						11,
						0,
						input);
				throw nvae;
			}

			switch (alt11) {
				case 1:
				// ECalc.g:144:4: 'true'
				{
					match("true");

				}
					break;
				case 2:
				// ECalc.g:145:4: 'false'
				{
					match("false");

				}
					break;

			}
			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}

	// $ANTLR end "BOOLEAN"

	// $ANTLR start "IDENT"
	public final void mIDENT()
			throws RecognitionException {
		try {
			int _type = IDENT;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// ECalc.g:154:2: ( ( 'a' .. 'z' | 'A' .. 'Z' | '_' ) ( 'a' .. 'z' |
			// 'A' .. 'Z' | '_' | '0' .. '9' )* )
			// ECalc.g:154:4: ( 'a' .. 'z' | 'A' .. 'Z' | '_' ) ( 'a' .. 'z' |
			// 'A' .. 'Z' | '_' | '0' .. '9' )*
			{
				if ((input.LA(1) >= 'A' && input.LA(1) <= 'Z') || input.LA(1) == '_' || (input.LA(1) >= 'a' && input.LA(1) <= 'z')) {
					input.consume();
				}
				else {
					MismatchedSetException mse = new MismatchedSetException(
							null,
							input);
					recover(mse);
					throw mse;
				}
				// ECalc.g:154:32: ( 'a' .. 'z' | 'A' .. 'Z' | '_' | '0' .. '9'
				// )*
				loop12: while (true) {
					int alt12 = 2;
					int LA12_0 = input.LA(1);
					if (((LA12_0 >= '0' && LA12_0 <= '9') || (LA12_0 >= 'A' && LA12_0 <= 'Z') || LA12_0 == '_' || (LA12_0 >= 'a' && LA12_0 <= 'z'))) {
						alt12 = 1;
					}

					switch (alt12) {
						case 1:
						// ECalc.g:
						{
							if ((input.LA(1) >= '0' && input.LA(1) <= '9') || (input.LA(1) >= 'A' && input.LA(1) <= 'Z') || input.LA(1) == '_' || (input.LA(1) >= 'a' && input.LA(1) <= 'z')) {
								input.consume();
							}
							else {
								MismatchedSetException mse = new MismatchedSetException(
										null,
										input);
								recover(mse);
								throw mse;
							}
						}
							break;

						default:
							break loop12;
					}
				}

			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}

	// $ANTLR end "IDENT"

	// $ANTLR start "EscapeSequence"
	public final void mEscapeSequence()
			throws RecognitionException {
		try {
			// ECalc.g:158:2: ( '\\\\' ( 'n' | 'r' | 't' | '\\'' | '\\\\' |
			// UnicodeEscape ) )
			// ECalc.g:158:4: '\\\\' ( 'n' | 'r' | 't' | '\\'' | '\\\\' |
			// UnicodeEscape )
			{
				match('\\');
				// ECalc.g:159:4: ( 'n' | 'r' | 't' | '\\'' | '\\\\' |
				// UnicodeEscape )
				int alt13 = 6;
				switch (input.LA(1)) {
					case 'n': {
						alt13 = 1;
					}
						break;
					case 'r': {
						alt13 = 2;
					}
						break;
					case 't': {
						alt13 = 3;
					}
						break;
					case '\'': {
						alt13 = 4;
					}
						break;
					case '\\': {
						alt13 = 5;
					}
						break;
					case 'u': {
						alt13 = 6;
					}
						break;
					default:
						NoViableAltException nvae = new NoViableAltException(
								"",
								13,
								0,
								input);
						throw nvae;
				}
				switch (alt13) {
					case 1:
					// ECalc.g:160:5: 'n'
					{
						match('n');
					}
						break;
					case 2:
					// ECalc.g:161:4: 'r'
					{
						match('r');
					}
						break;
					case 3:
					// ECalc.g:162:4: 't'
					{
						match('t');
					}
						break;
					case 4:
					// ECalc.g:163:4: '\\''
					{
						match('\'');
					}
						break;
					case 5:
					// ECalc.g:164:4: '\\\\'
					{
						match('\\');
					}
						break;
					case 6:
					// ECalc.g:165:4: UnicodeEscape
					{
						mUnicodeEscape();

					}
						break;

				}

			}

		}
		finally {
			// do for sure before leaving
		}
	}

	// $ANTLR end "EscapeSequence"

	// $ANTLR start "UnicodeEscape"
	public final void mUnicodeEscape()
			throws RecognitionException {
		try {
			// ECalc.g:170:6: ( 'u' HexDigit HexDigit HexDigit HexDigit )
			// ECalc.g:170:12: 'u' HexDigit HexDigit HexDigit HexDigit
			{
				match('u');
				mHexDigit();

				mHexDigit();

				mHexDigit();

				mHexDigit();

			}

		}
		finally {
			// do for sure before leaving
		}
	}

	// $ANTLR end "UnicodeEscape"

	// $ANTLR start "HexDigit"
	public final void mHexDigit()
			throws RecognitionException {
		try {
			// ECalc.g:174:2: ( ( '0' .. '9' | 'a' .. 'f' | 'A' .. 'F' ) )
			// ECalc.g:
			{
				if ((input.LA(1) >= '0' && input.LA(1) <= '9') || (input.LA(1) >= 'A' && input.LA(1) <= 'F') || (input.LA(1) >= 'a' && input.LA(1) <= 'f')) {
					input.consume();
				}
				else {
					MismatchedSetException mse = new MismatchedSetException(
							null,
							input);
					recover(mse);
					throw mse;
				}
			}

		}
		finally {
			// do for sure before leaving
		}
	}

	// $ANTLR end "HexDigit"

	// $ANTLR start "WS"
	public final void mWS()
			throws RecognitionException {
		try {
			int _type = WS;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// ECalc.g:178:2: ( ( ' ' | '\\r' | '\\t' | '\ ' | '\\n' ) )
			// ECalc.g:178:5: ( ' ' | '\\r' | '\\t' | '\ ' | '\\n' )
			{
				if ((input.LA(1) >= '\t' && input.LA(1) <= '\n') || (input.LA(1) >= '\f' && input.LA(1) <= '\r') || input.LA(1) == ' ') {
					input.consume();
				}
				else {
					MismatchedSetException mse = new MismatchedSetException(
							null,
							input);
					recover(mse);
					throw mse;
				}
				_channel = HIDDEN;
			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}

	// $ANTLR end "WS"

	@Override
	public void mTokens()
			throws RecognitionException {
		// ECalc.g:1:8: ( T__31 | T__32 | T__33 | T__34 | T__35 | OR | AND |
		// EQUALS | NOTEQUALS | LT | LTEQ | GT | GTEQ | PLUS | MINUS | MULT |
		// DIV | MOD | POW | NOT | STRING | INTEGER | FLOAT | DATETIME | BOOLEAN
		// | IDENT | WS )
		int alt14 = 27;
		alt14 = dfa14.predict(input);
		switch (alt14) {
			case 1:
			// ECalc.g:1:10: T__31
			{
				mT__31();

			}
				break;
			case 2:
			// ECalc.g:1:16: T__32
			{
				mT__32();

			}
				break;
			case 3:
			// ECalc.g:1:22: T__33
			{
				mT__33();

			}
				break;
			case 4:
			// ECalc.g:1:28: T__34
			{
				mT__34();

			}
				break;
			case 5:
			// ECalc.g:1:34: T__35
			{
				mT__35();

			}
				break;
			case 6:
			// ECalc.g:1:40: OR
			{
				mOR();

			}
				break;
			case 7:
			// ECalc.g:1:43: AND
			{
				mAND();

			}
				break;
			case 8:
			// ECalc.g:1:47: EQUALS
			{
				mEQUALS();

			}
				break;
			case 9:
			// ECalc.g:1:54: NOTEQUALS
			{
				mNOTEQUALS();

			}
				break;
			case 10:
			// ECalc.g:1:64: LT
			{
				mLT();

			}
				break;
			case 11:
			// ECalc.g:1:67: LTEQ
			{
				mLTEQ();

			}
				break;
			case 12:
			// ECalc.g:1:72: GT
			{
				mGT();

			}
				break;
			case 13:
			// ECalc.g:1:75: GTEQ
			{
				mGTEQ();

			}
				break;
			case 14:
			// ECalc.g:1:80: PLUS
			{
				mPLUS();

			}
				break;
			case 15:
			// ECalc.g:1:85: MINUS
			{
				mMINUS();

			}
				break;
			case 16:
			// ECalc.g:1:91: MULT
			{
				mMULT();

			}
				break;
			case 17:
			// ECalc.g:1:96: DIV
			{
				mDIV();

			}
				break;
			case 18:
			// ECalc.g:1:100: MOD
			{
				mMOD();

			}
				break;
			case 19:
			// ECalc.g:1:104: POW
			{
				mPOW();

			}
				break;
			case 20:
			// ECalc.g:1:108: NOT
			{
				mNOT();

			}
				break;
			case 21:
			// ECalc.g:1:112: STRING
			{
				mSTRING();

			}
				break;
			case 22:
			// ECalc.g:1:119: INTEGER
			{
				mINTEGER();

			}
				break;
			case 23:
			// ECalc.g:1:127: FLOAT
			{
				mFLOAT();

			}
				break;
			case 24:
			// ECalc.g:1:133: DATETIME
			{
				mDATETIME();

			}
				break;
			case 25:
			// ECalc.g:1:142: BOOLEAN
			{
				mBOOLEAN();

			}
				break;
			case 26:
			// ECalc.g:1:150: IDENT
			{
				mIDENT();

			}
				break;
			case 27:
			// ECalc.g:1:156: WS
			{
				mWS();

			}
				break;

		}
	}

	protected DFA14 dfa14 = new DFA14(
			this);
	static final String DFA14_eotS = "\7\uffff\1\33\1\uffff\1\33\1\uffff\1\40\1\42\1\44\6\uffff\1\33\1\uffff" + "\1\46\2\uffff\2\33\2\uffff\1\6\1\33\6\uffff\1\33\1\uffff\2\33\1\10\1\40" + "\2\33\1\57\1\33\1\uffff\1\57";
	static final String DFA14_eofS = "\61\uffff";
	static final String DFA14_minS = "\1\11\6\uffff\1\162\1\uffff\1\156\1\uffff\3\75\6\uffff\1\157\1\uffff\1" + "\56\2\uffff\1\162\1\141\2\uffff\1\60\1\144\6\uffff\1\164\1\uffff\1\165" + "\1\154\2\60\1\145\1\163\1\60\1\145\1\uffff\1\60";
	static final String DFA14_maxS = "\1\174\6\uffff\1\162\1\uffff\1\156\1\uffff\1\75\1\76\1\75\6\uffff\1\157" + "\1\uffff\1\71\2\uffff\1\162\1\141\2\uffff\1\172\1\144\6\uffff\1\164\1" + "\uffff\1\165\1\154\2\172\1\145\1\163\1\172\1\145\1\uffff\1\172";
	static final String DFA14_acceptS = "\1\uffff\1\1\1\2\1\3\1\4\1\5\1\6\1\uffff\1\7\1\uffff\1\10\3\uffff\1\16" + "\1\17\1\20\1\21\1\22\1\23\1\uffff\1\25\1\uffff\1\27\1\30\2\uffff\1\32" + "\1\33\2\uffff\1\11\1\24\1\13\1\12\1\15\1\14\1\uffff\1\26\10\uffff\1\31" + "\1\uffff";
	static final String DFA14_specialS = "\61\uffff}>";
	static final String[] DFA14_transitionS = {
		"\2\34\1\uffff\2\34\22\uffff\1\34\1\13\1\uffff\1\30\1\uffff\1\22\1\10" + "\1\25\1\1\1\2\1\20\1\16\1\3\1\17\1\27\1\21\12\26\2\uffff\1\14\1\12\1" + "\15\2\uffff\32\33\1\4\1\uffff\1\5\1\23\1\33\1\uffff\1\11\4\33\1\32\7" + "\33\1\24\1\7\4\33\1\31\6\33\1\uffff\1\6",
		"",
		"",
		"",
		"",
		"",
		"",
		"\1\35",
		"",
		"\1\36",
		"",
		"\1\37",
		"\1\41\1\37",
		"\1\43",
		"",
		"",
		"",
		"",
		"",
		"",
		"\1\45",
		"",
		"\1\27\1\uffff\12\26",
		"",
		"",
		"\1\47",
		"\1\50",
		"",
		"",
		"\12\33\7\uffff\32\33\4\uffff\1\33\1\uffff\32\33",
		"\1\51",
		"",
		"",
		"",
		"",
		"",
		"",
		"\1\52",
		"",
		"\1\53",
		"\1\54",
		"\12\33\7\uffff\32\33\4\uffff\1\33\1\uffff\32\33",
		"\12\33\7\uffff\32\33\4\uffff\1\33\1\uffff\32\33",
		"\1\55",
		"\1\56",
		"\12\33\7\uffff\32\33\4\uffff\1\33\1\uffff\32\33",
		"\1\60",
		"",
		"\12\33\7\uffff\32\33\4\uffff\1\33\1\uffff\32\33"
	};

	static final short[] DFA14_eot = DFA.unpackEncodedString(DFA14_eotS);
	static final short[] DFA14_eof = DFA.unpackEncodedString(DFA14_eofS);
	static final char[] DFA14_min = DFA.unpackEncodedStringToUnsignedChars(DFA14_minS);
	static final char[] DFA14_max = DFA.unpackEncodedStringToUnsignedChars(DFA14_maxS);
	static final short[] DFA14_accept = DFA.unpackEncodedString(DFA14_acceptS);
	static final short[] DFA14_special = DFA.unpackEncodedString(DFA14_specialS);
	static final short[][] DFA14_transition;

	static {
		int numStates = DFA14_transitionS.length;
		DFA14_transition = new short[numStates][];
		for (int i = 0; i < numStates; i++) {
			DFA14_transition[i] = DFA.unpackEncodedString(DFA14_transitionS[i]);
		}
	}

	protected class DFA14 extends
			DFA
	{

		public DFA14(
				BaseRecognizer recognizer ) {
			this.recognizer = recognizer;
			this.decisionNumber = 14;
			this.eot = DFA14_eot;
			this.eof = DFA14_eof;
			this.min = DFA14_min;
			this.max = DFA14_max;
			this.accept = DFA14_accept;
			this.special = DFA14_special;
			this.transition = DFA14_transition;
		}

		@Override
		public String getDescription() {
			return "1:1: Tokens : ( T__31 | T__32 | T__33 | T__34 | T__35 | OR | AND | EQUALS | NOTEQUALS | LT | LTEQ | GT | GTEQ | PLUS | MINUS | MULT | DIV | MOD | POW | NOT | STRING | INTEGER | FLOAT | DATETIME | BOOLEAN | IDENT | WS );";
		}
	}

}
