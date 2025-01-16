from flask import Flask, jsonify, request
import cx_Oracle
import datetime
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.events import EVENT_JOB_EXECUTED, EVENT_JOB_ERROR

app = Flask(__name__)

# Configurações de conexão com o Oracle
ORACLE_USERNAME = 'COBATA'
ORACLE_PASSWORD = 'C0BAT4D1T'
ORACLE_HOST = '192.168.0.254'
ORACLE_PORT = 1523
ORACLE_SID = 'WINT'

# Variáveis globais para armazenar dados atualizados
global_data_vwsomelier = []
global_data_pcpedc = []

# Função para conectar ao banco de dados
def connect_to_oracle():
    try:
        dsn = cx_Oracle.makedsn(ORACLE_HOST, ORACLE_PORT, sid=ORACLE_SID)
        return cx_Oracle.connect(ORACLE_USERNAME, ORACLE_PASSWORD, dsn)
    except cx_Oracle.DatabaseError as e:
        print(f"Erro ao conectar com o banco de dados: {e}")
        return None

# Função para consultar dados da tabela VW_SOMELIER com paginação
def get_oracle_data_paginated_vwsomelier(data_inicial, data_final, pagina, limite):
    try:
        connection = connect_to_oracle()
        if connection is None:
            return []

        cursor = connection.cursor()
        offset = (pagina - 1) * limite
        query = """
            SELECT 
                VS.DESCRICAO, 
                VS.CODPROD, 
                VS.DATA, 
                VS.QT, 
                VS.PVENDA, 
                VS.VLCUSTOFIN 
            FROM (
                SELECT 
                    DESCRICAO, 
                    CODPROD,
                    DATA, 
                    QT, 
                    PVENDA, 
                    VLCUSTOFIN, 
                    ROW_NUMBER() OVER (ORDER BY VS.DATA) AS row_num
                FROM VW_SOMELIER VS
                WHERE TRUNC(VS.DATA) BETWEEN :data_inicial AND :data_final
            ) VS
            WHERE VS.row_num > :offset AND VS.row_num <= :limit
        """

        params = {
            'data_inicial': data_inicial, 
            'data_final': data_final,
            'offset': offset,
            'limit': offset + limite
        }

        cursor.execute(query, params)
        rows = cursor.fetchall()
        cursor.close()
        connection.close()

        return rows
    except cx_Oracle.DatabaseError as e:
        print(f"Ocorreu um erro ao conectar ou executar a consulta: {e}")
        return []


    
# Função para consultar dados da tabela PCPEDC com paginação
def get_oracle_data_paginated_pcpedc(data_inicial, data_final, pagina, limite):
    try:
        connection = connect_to_oracle()
        if connection is None:
            return []

        cursor = connection.cursor()
        offset = (pagina - 1) * limite
        query = """
            SELECT 
                    PC.NUMPED, 
                    PC.DATA AS DATA_PEDIDO,  
                    PC.VLTOTAL,
                    PC.VLBONIFIC,  
                    PU.NOME AS CODUSUR,  -- Agora vai trazer o nome associado ao código do usuário
                    PC.CODFILIAL,
                    PR.PRACA AS CODPRACA,
                    PC.CODCLI AS 
                FROM (
                    SELECT 
                        NUMPED,
                        PC.DATA,  
                        VLTOTAL,
                        VLBONIFIC,  
                        CODUSUR,
                        CODFILIAL,
                        CODPRACA,
                        CODCLI,
                        ROW_NUMBER() OVER (ORDER BY PC.DATA) AS row_num
                    FROM PCPEDC PC
                    WHERE TRUNC(PC.DATA) BETWEEN :data_inicial AND :data_final  
                ) PC
                JOIN PCPRACA PR ON PC.CODPRACA = PR.CODPRACA  
                JOIN PCUSUARI PU ON PC.CODUSUR = PU.CODUSUR  -- Realiza o JOIN com a tabela PCUSUARI para obter o nome
                WHERE PC.row_num > :offset AND PC.row_num <= :limit
        """

        params = {
            'data_inicial': data_inicial, 
            'data_final': data_final,
            'offset': offset,
            'limit': offset + limite
        }

        cursor.execute(query, params)
        rows = cursor.fetchall()
        cursor.close()
        connection.close()

        # Filtra os dados, excluindo os casos onde VLBONIFIC é maior que 0
        filtered_rows = [row for row in rows if row[3] == 0]  # VLBONIFIC == 0

        return filtered_rows
    except cx_Oracle.DatabaseError as e:
        print(f"Ocorreu um erro ao conectar ou executar a consulta: {e}")
        return []

# Função que será executada a cada 3 minutos para atualizar os dados
def atualizar_dados():
    data_inicial = datetime.date.today() - datetime.timedelta(days=7)  # 7 dias atrás
    data_final = datetime.date.today()  # Data de hoje

    # Atualizar os dados das duas tabelas
    print(f"Atualizando dados entre {data_inicial} e {data_final}")
    
    global global_data_vwsomelier, global_data_pcpedc
    global_data_vwsomelier = get_oracle_data_paginated_vwsomelier(data_inicial, data_final, 1, 10)
    global_data_pcpedc = get_oracle_data_paginated_pcpedc(data_inicial, data_final, 1, 10)

    print("Dados atualizados.")

# Função para configurar o agendador
def setup_scheduler():
    scheduler = BackgroundScheduler()
    scheduler.add_job(atualizar_dados, 'interval', seconds=10)  # Atualizando a cada 30 segundos

    # Adiciona listener para capturar erros e execuções
    def job_listener(event):
        if event.exception:
            print(f"Erro ao executar o job {event.job_id}")
        else:
            print(f"Job {event.job_id} executado com sucesso.")

    scheduler.add_listener(job_listener, EVENT_JOB_EXECUTED | EVENT_JOB_ERROR)

    # Inicia o agendador
    scheduler.start()
    print("Agendador iniciado.")

# Endpoint para acessar os dados da tabela VW_SOMELIER
@app.route('/dados_vwsomelier', methods=['GET'])
def get_data_vwsomelier():
    data_inicial_str = request.args.get('data_inicial')
    data_final_str = request.args.get('data_final')

    if not data_inicial_str or not data_final_str:
        return jsonify({"error": "Parâmetros de data_inicial e data_final são obrigatórios."}), 400

    try:
        data_inicial = datetime.datetime.strptime(data_inicial_str, "%Y-%m-%d").date()
        data_final = datetime.datetime.strptime(data_final_str, "%Y-%m-%d").date()
    except ValueError:
        return jsonify({"error": "Formato de data inválido. Use o formato YYYY-MM-DD."}), 400

    pagina = int(request.args.get('pagina', 1))
    limite = int(request.args.get('limite', 5000000))

    rows = get_oracle_data_paginated_vwsomelier(data_inicial, data_final, pagina, limite)

    if not rows:
        return jsonify({"message": "Nenhum dado encontrado para o intervalo de datas fornecido."}), 404

    results = []
    for row in rows:
        results.append({
            'DESCRICAO': row[0],
            'CODPROD': row[1],
            'DATA': row[2].strftime('%Y-%m-%d') if isinstance(row[2], datetime.date) else row[2],
            'QT': row[3],
            'PVENDA': row[4],
            'VLCUSTOFIN': row[5]
        })

    return jsonify(results)

# Endpoint para acessar os dados da tabela PCPEDC
@app.route('/dados_pcpedc', methods=['GET'])
def get_data_pcpedc():
    data_inicial_str = request.args.get('data_inicial')
    data_final_str = request.args.get('data_final')

    if not data_inicial_str or not data_final_str:
        return jsonify({"error": "Parâmetros de data_inicial e data_final são obrigatórios."}), 400

    try:
        data_inicial = datetime.datetime.strptime(data_inicial_str, "%Y-%m-%d").date()
        data_final = datetime.datetime.strptime(data_final_str, "%Y-%m-%d").date()
    except ValueError:
        return jsonify({"error": "Formato de data inválido. Use o formato YYYY-MM-DD."}), 400

    pagina = int(request.args.get('pagina', 1))
    limite = int(request.args.get('limite', 5000000))

    rows = get_oracle_data_paginated_pcpedc(data_inicial, data_final, pagina, limite)

    if not rows:
        return jsonify({"message": "Nenhum dado encontrado para o intervalo de datas fornecido."}), 404

    results = []
    for row in rows:
        results.append({
            'NUMPED': row[0],
            'DATA': row[1].strftime('%Y-%m-%d') if isinstance(row[1], datetime.date) else row[1],
            'VLTOTAL': row[2],
            'VLBONIFIC': row[3],
            'NOME': row[4],
            'CODFILIAL': row[5],
            'CODPRACA': row[6],
            'CODCLI': row[7]
            
        })

    return jsonify(results)

# Iniciar o servidor Flask
if __name__ == '__main__':
    setup_scheduler()  # Inicializa o agendador
    atualizar_dados()  # Atualiza os dados ao iniciar o servidor
    app.run(debug=True)