grammar GWQL;

options {
	language = Java;
}

@header {
import java.util.List;

import org.geotools.filter.text.cql2.CQLException;
import org.geotools.filter.text.ecql.ECQL;
import org.opengis.filter.Filter;
import com.google.common.collect.Lists;

import org.locationtech.geowave.adapter.vector.query.gwql.*;
import org.locationtech.geowave.adapter.vector.query.gwql.statement.*;
}

@parser::members {
public static Statement parseStatement(final String statement) {
	GWQLLexer lexer = new GWQLLexer(CharStreams.fromString(statement));
	TokenStream tokenStream = new CommonTokenStream(lexer);
	GWQLParser parser = new GWQLParser(tokenStream);
	parser.removeErrorListeners();
	parser.addErrorListener(new ErrorListener());
	return parser.query().stmt;
}
}

query
	returns [
		Statement stmt
	]
 	: statement (';')* EOF
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
 	: select_statement
 	{
 		$stmt = $select_statement.stmt;
 	}
 	| delete_statement
 	{
 		$stmt = $delete_statement.stmt;
 	}
;

delete_statement
	returns [
		DeleteStatement stmt
	]
	locals [
		Filter filter = null
	]
 	: K_DELETE K_FROM qualified_type_name 
	( K_WHERE cql_filter { $filter = $cql_filter.filter; })?
   	{
   		$stmt = new DeleteStatement($qualified_type_name.qtn, $filter);
   	}
;

select_statement
	returns [
		SelectStatement stmt
	]
	locals [
		Filter filter = null,
		Integer limit = null,
		List<Selector> selectorList = Lists.newArrayList()
	]
 	: K_SELECT selectors[$selectorList]
	  K_FROM qualified_type_name
	( K_WHERE cql_filter { $filter = $cql_filter.filter; })?
	( K_LIMIT INTEGER { $limit = $INTEGER.int; })?
	{
		$stmt = new SelectStatement($qualified_type_name.qtn, $selectorList, $filter, $limit);
	}
;
 
error
	: UNEXPECTED_CHAR
   	{ 
    	throw new RuntimeException("UNEXPECTED_CHAR=" + $UNEXPECTED_CHAR.text); 
   	}
;
 
qualified_type_name
	returns [
		QualifiedTypeName qtn
	]
 	: store_name '.' table_name
 	{
 		$qtn = new QualifiedTypeName($store_name.text, $table_name.text);
 	}
;

selectors [List<Selector> selectorList]
	: agg1=aggregate { $selectorList.add($agg1.sel); } (',' aggN=aggregate { $selectorList.add($aggN.sel); } )*
	| sel1=selector { $selectorList.add($sel1.sel); } (',' selN=selector { $selectorList.add($selN.sel); } )*
	| '*'
;

selector
	returns [
		ColumnSelector sel
	]
	locals [
		String alias = null
	]
	: column_name 
	( K_AS column_alias { $alias = $column_alias.text; } )?
	{
		$sel = new ColumnSelector($column_name.text, $alias);
	}
;

 
aggregate
	returns [
		AggregationSelector sel
	]
	locals [
		String alias = null
	]
	: function_name '(' function_arg ')'
	( K_AS column_alias { $alias = $column_alias.text; } )?
	{
		$sel = new AggregationSelector($function_name.text, new String[] { $function_arg.text }, $alias);
	}
;

function_arg
	: '*'
	| column_name
;
 
column_name
	: IDENTIFIER
;

column_alias
	: IDENTIFIER
;
 
store_name
	: IDENTIFIER
;
 
table_name
 	: IDENTIFIER
;
 
function_name
	: IDENTIFIER
;
 
cql_filter
	returns [
		Filter filter
	]
	locals [
		int openCount = 1
	]
	: K_CQL cql_paren
 	{
 	    try {
      		$filter = ECQL.toFilter($cql_paren.text);
	    } catch (CQLException e) {
	    	throw new RuntimeException("Invalid CQL", e);
	    }
 	}
;

cql_paren
	: '(' ( ~('(' | ')') | cql_paren )* ')'
;

keyword
	: K_AND
	| K_DELETE
	| K_FROM
	| K_LIMIT
	| K_OR
	| K_SELECT
	| K_WHERE
;

K_AND : A N D;
K_AS : A S;
K_CQL : C Q L;
K_DELETE : D E L E T E;
K_FROM : F R O M;
K_LIMIT : L I M I T;
K_OR : O R;
K_SELECT : S E L E C T;
K_WHERE : W H E R E;

IDENTIFIER
	: '"' (~'"' | '""')* '"'
	| '`' (~'`' | '``')* '`'
 	| '[' ~']'* ']'
 	| [a-zA-Z_] [a-zA-Z_0-9]* // TODO check: needs more chars in set
;
 
INTEGER
	: DIGIT+
;
 
NUMERIC_LITERAL
 	: DIGIT+ ( '.' DIGIT* )? ( E [-+]? DIGIT+ )?
 	| '.' DIGIT+ ( E [-+]? DIGIT+ )?
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