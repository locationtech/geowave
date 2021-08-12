grammar GWQL;

options {
	language = Java;
}

@parser::header {
import java.util.List;

import com.google.common.collect.Lists;

import org.locationtech.geowave.core.store.api.DataStore;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.query.gwql.ErrorListener;
import org.locationtech.geowave.core.store.query.gwql.GWQLParseHelper;
import org.locationtech.geowave.core.store.query.gwql.GWQLParseException;
import org.locationtech.geowave.core.store.query.gwql.AggregationSelector;
import org.locationtech.geowave.core.store.query.gwql.ColumnSelector;
import org.locationtech.geowave.core.store.query.gwql.Selector;
import org.locationtech.geowave.core.store.query.gwql.statement.Statement;
import org.locationtech.geowave.core.store.query.gwql.statement.SelectStatement;
import org.locationtech.geowave.core.store.query.gwql.statement.DeleteStatement;
import org.locationtech.geowave.core.store.query.filter.expression.Filter;
import org.locationtech.geowave.core.store.query.filter.expression.Predicate;
import org.locationtech.geowave.core.store.query.filter.expression.Expression;
import org.locationtech.geowave.core.store.query.filter.expression.Literal;
import org.locationtech.geowave.core.store.query.filter.expression.FieldValue;
import org.locationtech.geowave.core.store.query.filter.expression.BooleanLiteral;
import org.locationtech.geowave.core.store.query.filter.expression.numeric.NumericLiteral;
import org.locationtech.geowave.core.store.query.filter.expression.text.TextLiteral;
}

@parser::members {
	private DataStore dataStore = null;
	private DataTypeAdapter<?> adapter = null;
	public static Statement parseStatement(final DataStore dataStore, final String statement) {
		final GWQLLexer lexer = new GWQLLexer(CharStreams.fromString(statement));
		final TokenStream tokenStream = new CommonTokenStream(lexer);
		final GWQLParser parser = new GWQLParser(tokenStream);
		parser.dataStore = dataStore;
		parser.removeErrorListeners();
		parser.addErrorListener(new ErrorListener());
		return parser.query().stmt;
	}
}

query
	returns [
		Statement stmt
	]
 	: statement (SEMICOLON)* EOF
 	{
 		$stmt = $statement.stmt;
 	}
 	| error
 	{
 		$stmt=null;
 	}
;

statement
	returns [
		Statement stmt
	]
 	: selectStatement
 	{
 		$stmt = $selectStatement.stmt;
 	}
 	| deleteStatement
 	{
 		$stmt = $deleteStatement.stmt;
 	}
;

deleteStatement
	returns [
		DeleteStatement stmt
	]
	locals [
		Filter f = null
	]
 	: K_DELETE K_FROM adapterName 
	( K_WHERE filter { $f = $filter.value; })?
   	{
   		$stmt = new DeleteStatement(dataStore, adapter, $f);
   	}
;

selectStatement
	returns [
		SelectStatement stmt
	]
	locals [
		Filter f = null,
		Integer limit = null,
		List<Selector> selectorList = Lists.newArrayList()
	]
 	: K_SELECT selectors[$selectorList]
	  K_FROM adapterName
	( K_WHERE filter { $f = $filter.value; })?
	( K_LIMIT INTEGER { $limit = $INTEGER.int; })?
	{
		$stmt = new SelectStatement(dataStore, adapter, $selectorList, $f, $limit);
	}
;
 
error
	: UNEXPECTED_CHAR
   	{ 
    	throw new GWQLParseException("UNEXPECTED_CHAR=" + $UNEXPECTED_CHAR.text); 
   	}
;

selectors [List<Selector> selectorList]
	: agg1=aggregate { $selectorList.add($agg1.sel); } (COMMA aggN=aggregate { $selectorList.add($aggN.sel); } )*
	| sel1=selector { $selectorList.add($sel1.sel); } (COMMA selN=selector { $selectorList.add($selN.sel); } )*
	| '*'
;

selector
	returns [
		ColumnSelector sel
	]
	locals [
		String alias = null
	]
	: columnName 
	( K_AS columnAlias { $alias = $columnAlias.text; } )?
	{
		$sel = new ColumnSelector($columnName.text, $alias);
	}
;

 
aggregate
	returns [
		AggregationSelector sel
	]
	locals [
		String alias = null
	]
	: functionName '(' functionArg ')'
	( K_AS columnAlias { $alias = $columnAlias.text; } )?
	{
		$sel = new AggregationSelector($functionName.text, new String[] { $functionArg.text }, $alias);
	}
;

functionArg
	: '*'
	| columnName
;

adapterName
	: tableName
	{
		adapter = dataStore.getType($tableName.text);
		if (adapter == null) {
			throw new GWQLParseException("No type named " + $tableName.text);
		}
	}
;
 
columnName
	: IDENTIFIER
;

columnAlias
	: IDENTIFIER
;
 
tableName
 	: IDENTIFIER
;
 
functionName
	: IDENTIFIER
;

filter
	returns [
		Filter value
	]
	: predicate { $value = $predicate.value; }                          #simplePredicateFilter
	| f1=filter K_AND f2=filter { $value = $f1.value.and($f2.value); }  #andFilter
	| f1=filter K_OR f2=filter { $value = $f1.value.or($f2.value); }    #orFilter
	| K_NOT f=filter { $value = Filter.not($f.value); }                 #notFilter
	| LPAREN f=filter RPAREN { $value = $f.value; }                     #parenFilter
	| LSQUARE f=filter RSQUARE { $value = $f.value; }					#sqBracketFilter
	| K_INCLUDE { $value = Filter.include(); }						    #includeFilter
	| K_EXCLUDE { $value = Filter.exclude(); }							#excludeFilter
;

predicate
	returns [
		Predicate value
	]
	: f=predicateFunction { $value = $f.value; }
	| e1=expression EQUALS e2=expression { $value = GWQLParseHelper.getEqualsPredicate($e1.value, $e2.value); }
	| e1=expression NOT_EQUALS e2=expression { $value = GWQLParseHelper.getNotEqualsPredicate($e1.value, $e2.value); }
	| e1=expression LESS_THAN e2=expression { $value = GWQLParseHelper.getLessThanPredicate($e1.value, $e2.value); }
	| e1=expression LESS_THAN_OR_EQUAL e2=expression { $value = GWQLParseHelper.getLessThanOrEqualsPredicate($e1.value, $e2.value); }
	| e1=expression GREATER_THAN e2=expression { $value = GWQLParseHelper.getGreaterThanPredicate($e1.value, $e2.value); }
	| e1=expression GREATER_THAN_OR_EQUAL e2=expression { $value = GWQLParseHelper.getGreaterThanOrEqualsPredicate($e1.value, $e2.value); }
	| v=expression K_BETWEEN l=expression K_AND u=expression { $value = GWQLParseHelper.getBetweenPredicate($v.value, $l.value, $u.value); }
	| e=expression K_IS K_NULL { $value = $e.value.isNull(); }
	| e=expression K_IS K_NOT K_NULL { $value = $e.value.isNotNull(); }
	| e1=expression o=predicateOperator e2=expression { $value = GWQLParseHelper.getOperatorPredicate($o.text, $e1.value, $e2.value); }
;

expression
	returns [
		Expression<?> value
	]
	: e1=expression STAR e2=expression { $value = GWQLParseHelper.getMultiplyExpression($e1.value, $e2.value); }
	| e1=expression DIVIDE e2=expression { $value = GWQLParseHelper.getDivideExpression($e1.value, $e2.value); }
	| e1=expression PLUS e2=expression { $value = GWQLParseHelper.getAddExpression($e1.value, $e2.value); }
	| e1=expression MINUS e2=expression { $value = GWQLParseHelper.getSubtractExpression($e1.value, $e2.value); }
	| f=expressionFunction { $value = $f.value; }
	| LPAREN e=expression RPAREN { $value = $e.value; }
	| LSQURE e=expression RSQUARE { $value = $e.value; }
	| e1=expression CAST IDENTIFIER { $value = GWQLParseHelper.castExpression($IDENTIFIER.text, $e1.value); }
	| l=literal { $value = $l.value; }
	| c=columnName { $value = GWQLParseHelper.getFieldValue(adapter, $c.text); }
;

predicateFunction
	returns [
		Predicate value
	]
	locals [
		List<Expression<?>> expressions = Lists.newArrayList()
	]
	: functionName LPAREN expressionList[$expressions] RPAREN { $value = GWQLParseHelper.getPredicateFunction($functionName.text, $expressions); }
;

expressionFunction
	returns [
		Expression<?> value
	]
	locals [
		List<Expression<?>> expressions = Lists.newArrayList()
	]
	: functionName LPAREN expressionList[$expressions] RPAREN { $value = GWQLParseHelper.getExpressionFunction($functionName.text, $expressions); }
;

predicateOperator
	: IDENTIFIER
;

expressionList [List<Expression<?>> expressions]
	: expr1=expression { $expressions.add($expr1.value); } (COMMA exprN=expression { $expressions.add($exprN.value); } )*
;
	
literal
	returns [
		Literal value
	]
	: number { $value = NumericLiteral.of(Double.parseDouble($number.text)); }
	| textLiteral { $value = $textLiteral.value; }
	| BOOLEAN_LITERAL { $value = BooleanLiteral.of($BOOLEAN_LITERAL.text); }
;

number
	: NUMERIC
	| INTEGER
;


textLiteral
    returns [
    	TextLiteral value
    ]
	: SQUOTE_LITERAL { $value = GWQLParseHelper.evaluateTextLiteral($SQUOTE_LITERAL.text); }
;

SQUOTE_LITERAL: '\'' ('\\'. | '\'\'' | ~('\'' | '\\'))* '\'';

ESCAPED_SQUOTE: BACKSLASH SQUOTE;
NEWLINE: BACKSLASH 'n';
RETURN: BACKSLASH 'r';
TAB: BACKSLASH 't';
BACKSPACE: BACKSLASH 'b';
FORM_FEED: BACKSLASH 'f';
ESCAPED_BACKSLASH: BACKSLASH BACKSLASH;

NOT_EQUALS: '<>';
LESS_THAN_OR_EQUAL: '<=';
GREATER_THAN_OR_EQUAL: '>=';
LESS_THAN: '<';
GREATER_THAN: '>';
EQUALS: '=';
LPAREN: '(';
RPAREN: ')';
LCURL: '{';
RCURL: '}';
LSQUARE: '[';
RSQUARE: ']';
COMMA: ',';
STAR: '*';
DIVIDE: '/';
PLUS: '+';
MINUS: '-';
CAST: '::';
DOT: '.';
SQUOTE: '\'';
DQUOTE: '"';
BACKSLASH: '\\';
SEMICOLON: ';';

K_AND : A N D;
K_AS : A S;
K_DELETE : D E L E T E;
K_FROM : F R O M;
K_LIMIT : L I M I T;
K_OR : O R;
K_SELECT : S E L E C T;
K_WHERE : W H E R E;
K_NOT : N O T;
K_IS : I S;
K_NULL : N U L L;
K_INCLUDE : I N C L U D E;
K_EXCLUDE: E X C L U D E;
K_BETWEEN: B E T W E E N;

BOOLEAN_LITERAL
	: T R U E
	| F A L S E
;

IDENTIFIER
	: ESCAPED_IDENTIFIER
	{
		String txt = getText();
		// strip the leading and trailing characters that wrap the identifier when using unconventional naming
		txt = txt.substring(1, txt.length() - 1); 
		setText(txt);
    }
    | [a-zA-Z_] [a-zA-Z0-9_]* // TODO check: needs more chars in set
;

ESCAPED_IDENTIFIER
	: '"' (~'"' | '""')* '"'
	| '`' (~'`' | '``')* '`'
	| '[' ~']'* ']'
;

INTEGER
	: MINUS? DIGIT+ (E DIGIT+)?
;

NUMERIC
 	: MINUS? DIGIT+ DOT DIGIT+ (E (MINUS)* DIGIT+)?
;

WHITESPACE: [ \t\n\r\f] -> channel(HIDDEN);

UNEXPECTED_CHAR
 	: .
;

fragment DIGIT : [0-9];

fragment A : [aA];
fragment B : [bB];
fragment C : [cC];
fragment D : [dD];
fragment E : [eE];
fragment F : [fF];
fragment G : [gG];
fragment H : [hH];
fragment I : [iI];
fragment J : [jJ];
fragment K : [kK];
fragment L : [lL];
fragment M : [mM];
fragment N : [nN];
fragment O : [oO];
fragment P : [pP];
fragment Q : [qQ];
fragment R : [rR];
fragment S : [sS];
fragment T : [tT];
fragment U : [uU];
fragment V : [vV];
fragment W : [wW];
fragment X : [xX];
fragment Y : [yY];
fragment Z : [zZ];