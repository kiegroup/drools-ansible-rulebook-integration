grammar Protoextractor;

extractor : identifier ( chunk )* ;

chunk : ('.' identifier) | squaredAccessor | indexAccessor ;

squaredAccessor : '[' stringLiteral ']';
indexAccessor : '[' integerLiteral ']';

stringLiteral : STRING1 | STRING2 ;
integerLiteral : SIGNED_INT ;

// keep as := ID given this is matching upstream pyparsing, see lexer rule.
identifier : ID ;

// ref https://pyparsing-docs.readthedocs.io/en/latest/pyparsing.html#pyparsing.pyparsing_common.identifier
ID : [\p{Letter}_][\p{Letter}_0-9]* ;

// ref https://github.com/ansible/ansible-rulebook/blob/24444bfcefcd7e9551d708723b76f58c3de9e976/ansible_rulebook/condition_parser.py#L118-L123
STRING1 : '\'' ~('\r' | '\n' | '"')* '\'' ;
STRING2 : '"' ~('\r' | '\n' | '"')* '"' ;

SIGNED_INT : (MINUS|PLUS)? DIGITS;
MINUS : '-';
PLUS : '+';
DIGITS : [0-9]+;
