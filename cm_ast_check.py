import ast
s=open('concurrent_multi_backtest.py','r',encoding='utf-8',errors='replace').read()
try:
    ast.parse(s)
    print('AST OK')
except SyntaxError as e:
    print('AST ERROR', e)
