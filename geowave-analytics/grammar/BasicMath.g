grammar BasicMath;
options 
{
	output=AST;
	ASTLabelType=CommonTree;
	language=Java;
}

tokens
{
	PARAM;
	NEGATE;
	STRING;
}

@members {
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
}

@header {
}


expression
	: 	logicalExpression EOF!
	;
		
logicalExpression
	:	booleanAndExpression (OR^ booleanAndExpression )* 
	;

OR 	: 	'||' | 'or';
	
booleanAndExpression
	:	equalityExpression (AND^ equalityExpression)* 
	;

AND 	: 	'&&' | 'and';

equalityExpression
	:	relationalExpression ((EQUALS|NOTEQUALS)^ relationalExpression)*
	;

EQUALS	
	:	'=' | '==';
NOTEQUALS 
	:	'!=' | '<>';

relationalExpression
	:	additiveExpression ( (LT|LTEQ|GT|GTEQ)^ additiveExpression )*
	;

LT	:	'<';
LTEQ	:	'<=';
GT	:	'>';
GTEQ	:	'>=';

additiveExpression
	:	multiplicativeExpression ( (PLUS|MINUS)^ multiplicativeExpression )*
	;

PLUS	:	'+';
MINUS	:	'-';

multiplicativeExpression 
	:	powerExpression ( (MULT|DIV|MOD)^ powerExpression )*
	;
	
MULT	:	'*';
DIV	:	'/';
MOD	:	'%';

powerExpression 
	:	unaryExpression ( POW^ unaryExpression )*
	;
	
POW	:	'^';

unaryExpression
	:	primaryExpression
    	|	NOT^ primaryExpression
    	|	MINUS primaryExpression -> ^(NEGATE primaryExpression)
   	;
  
NOT	:	'!' | 'not';

primaryExpression
	:	'('! logicalExpression ')'!
	|	value 
	;

value	
	: 	INTEGER
	|	FLOAT
	| 	DATETIME
	|	BOOLEAN
	|	STRING
	|	function
	|	parameter
	;

parameter
	:	'[' (IDENT|INTEGER) ']' -> ^(PARAM IDENT? INTEGER?)
	;
	
STRING
    	:  	'\'' ( EscapeSequence | (options {greedy=false;} : ~('\u0000'..'\u001f' | '\\' | '\'' ) ) )* '\''
    	;

INTEGER	
	:	('0'..'9')+
	;

FLOAT
	:	('0'..'9')* '.' ('0'..'9')+
	;

DATETIME
 	:	'#' (~'#')* '#'
        ;

BOOLEAN
	:	'true'
	|	'false'
	;
	
// Must be declared after the BOOLEAN token or it will hide it
function
	:	IDENT '(' ( logicalExpression (',' logicalExpression)* )? ')' -> ^(IDENT logicalExpression*)
	;

IDENT
	:	('a'..'z' | 'A'..'Z' | '_') ('a'..'z' | 'A'..'Z' | '_' | '0'..'9')*
	;

fragment EscapeSequence 
	:	'\\'
  	(	
  		'n' 
	|	'r' 
	|	't'
	|	'\'' 
	|	'\\'
	|	UnicodeEscape
	)
  ;

fragment UnicodeEscape
    	:    	'u' HexDigit HexDigit HexDigit HexDigit 
    	;

fragment HexDigit 
	: 	('0'..'9'|'a'..'f'|'A'..'F') ;

/* Ignore white spaces */	
WS	
	:  (' '|'\r'|'\t'|'\u000C'|'\n') {$channel=HIDDEN;}
	;
