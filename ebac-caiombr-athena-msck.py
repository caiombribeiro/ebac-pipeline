import boto3

def lambda_handler(event, context):
    
    # Nome do banco de dados e da tabela no AWS Athena
    database_name = 'seu_banco_de_dados'
    table_name = 'sua_tabela'
    
    # Inicializar o cliente do AWS Athena
    athena_client = boto3.client('athena')
    
    # Query MSCK REPAIR TABLE para reparar as partições
    query = f"MSCK REPAIR TABLE `{database_name}`.`{table_name}`;"
    
    try:
        # Iniciar a execução da consulta no AWS Athena
        response = athena_client.start_query_execution(
            QueryString=query,
            QueryExecutionContext={
                'Database': database_name
            },
            ResultConfiguration={
                'OutputLocation': 's3://ebac-caiom-query-results/'  # Local onde será armazenado o resultado da consulta
            }
        )
        
        query_execution_id = response['QueryExecutionId']
        print(f"Consulta iniciada. QueryExecutionId: {query_execution_id}")
        
        # Você pode adicionar aqui qualquer lógica adicional ou tratamento dos resultados, se necessário
        
    except Exception as e:
        print(f"Erro ao executar a consulta: {str(e)}")
        raise

    return {
        'statusCode': 200,
        'body': 'Comando MSCK REPAIR TABLE executado com sucesso.'
    }