SELECT 
    cli.nome AS Cliente,
    SUM(t.valor_total * (c.percentual / 100)) AS GanhoTotalPorCliente /* Calcula o ganho total por cliente. Multiplica o valor_total de cada transação pelo percentual do contrato correspondente e soma esses valores para cada cliente. */
FROM 
    transacao t
JOIN 
    contrato c ON t.contrato_id = c.contrato_id /* Junta as tabelas transacao e contrato com base no contrato_id. */
JOIN 
    cliente cli ON c.cliente_id = cli.cliente_id /* Junta a tabela contrato com a tabela cliente para obter o nome do cliente. */
WHERE 
    c.ativo = 1 /* Considera apenas os contratos que estão ativos. */
GROUP BY 
    cli.nome; /* Agrupa os resultados por cliente. */
