import os
import logging
import json
import base64
import requests
from requests.auth import HTTPDigestAuth
import os.path
from flask import Flask, request, jsonify
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from notion_client import Client
from dotenv import load_dotenv
from google.auth.transport.requests import Request

# Setup
app = Flask(__name__)
load_dotenv()

# Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Validate environment variables
required_env_vars = [
    "NOTION_TOKEN", "NOTION_DB_APONTAMENTOS", "NOTION_DB_USUARIOS",
    "NOTION_DB_OBRAS", "GOOGLE_DRIVE_FOLDER_ID", "GOOGLE_CREDENTIALS_BASE64", "KOBO_TOKEN"
]
optional_env_vars = ["KOBO_USERNAME", "KOBO_PASSWORD", "KOBO_MEDIA_TOKEN"]

for var in required_env_vars + optional_env_vars:
    value = os.getenv(var)
    if not value and var in required_env_vars:
        logger.error(f"Missing environment variable: {var}")
        raise ValueError(f"Environment variable {var} is not set")

# Decode credentials
GOOGLE_CREDENTIALS = json.loads(base64.b64decode(os.getenv("GOOGLE_CREDENTIALS_BASE64")).decode('utf-8'))

NOTION_TOKEN = os.getenv("NOTION_TOKEN")
NOTION_DB_APONTAMENTOS = os.getenv("NOTION_DB_APONTAMENTOS")
NOTION_DB_USUARIOS = os.getenv("NOTION_DB_USUARIOS")
NOTION_DB_OBRAS = os.getenv("NOTION_DB_OBRAS")
DRIVE_FOLDER_ID = os.getenv("GOOGLE_DRIVE_FOLDER_ID")
KOBO_TOKEN = os.getenv("KOBO_TOKEN")
KOBO_MEDIA_TOKEN = os.getenv("KOBO_MEDIA_TOKEN", KOBO_TOKEN)
KOBO_USERNAME = os.getenv("KOBO_USERNAME")
KOBO_PASSWORD = os.getenv("KOBO_PASSWORD")

# Initialize clients
notion = Client(auth=NOTION_TOKEN)
creds = service_account.Credentials.from_service_account_info(
    GOOGLE_CREDENTIALS, scopes=['https://www.googleapis.com/auth/drive']
)
if not creds.valid:
    creds.refresh(Request())
drive_service = build('drive', 'v3', credentials=creds)

# Função custom para query em database (bypass do problema do SDK)
def query_notion_database(database_id, filter_dict):
    url = f"https://api.notion.com/v1/databases/{database_id}/query"
    headers = {
        "Authorization": f"Bearer {NOTION_TOKEN}",
        "Content-Type": "application/json",
        "Notion-Version": "2022-06-28"
    }
    payload = {"filter": filter_dict} if filter_dict else {}
    response = requests.post(url, headers=headers, json=payload)
    if response.status_code == 200:
        return response.json()
    else:
        logger.error(f"[NOTION] Query error {response.status_code}: {response.text}")
        return {"results": []}

def upload_para_drive(nome_arquivo, download_url):
    try:
        diretorio = "fotos_recebidas"
        if not os.path.exists(diretorio):
            os.makedirs(diretorio)
        caminho = os.path.join(diretorio, nome_arquivo)

        headers = {}
        auth = None
        if KOBO_USERNAME and KOBO_PASSWORD:
            auth = HTTPDigestAuth(KOBO_USERNAME, KOBO_PASSWORD)
        elif KOBO_MEDIA_TOKEN and KOBO_MEDIA_TOKEN != KOBO_TOKEN:
            headers = {'Authorization': f'Bearer {KOBO_MEDIA_TOKEN}'}
        elif KOBO_TOKEN:
            headers = {'Authorization': f'Bearer {KOBO_TOKEN}'}

        response = requests.get(download_url, headers=headers, auth=auth, stream=True, timeout=30)
        if response.status_code != 200:
            logger.error(f"[DRIVE] Falha ao baixar {nome_arquivo}: {response.status_code}")
            return None

        with open(caminho, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)

        media = MediaFileUpload(caminho, resumable=True)
        arquivo_metadata = {'name': nome_arquivo, 'parents': [DRIVE_FOLDER_ID]}
        arquivo = drive_service.files().create(body=arquivo_metadata, media_body=media, fields='id').execute()
        file_id = arquivo.get('id')
        link = f"https://drive.google.com/file/d/{file_id}/view"
        logger.info(f"[DRIVE] Upload concluído: {link}")
        return link
    except Exception as e:
        logger.error(f"[DRIVE] Erro no upload: {e}")
        return None

def obter_usuario_por_login(login):
    try:
        response = query_notion_database(NOTION_DB_USUARIOS, {"property": "Título", "title": {"equals": login}})
        resultados = response.get("results", [])
        if resultados:
            return resultados[0]["id"]
        new_user = notion.pages.create(
            parent={"database_id": NOTION_DB_USUARIOS},
            properties={"Título": {"title": [{"text": {"content": login}}]}}
        )
        logger.info(f"[NOTION] Usuário criado: {login}")
        return new_user["id"]
    except Exception as e:
        logger.error(f"[NOTION] Erro usuário: {e}")
        return None

def obter_obra_id(obra_nome):
    try:
        response = query_notion_database(NOTION_DB_OBRAS, {"property": "Título", "title": {"equals": obra_nome}})
        resultados = response.get("results", [])
        if resultados:
            logger.info(f"[NOTION] Obra encontrada: {obra_nome}")
            return resultados[0]["id"]
        else:
            logger.warning(f"[NOTION] Obra não encontrada: {obra_nome}")
            return None  # Não cria automaticamente — retorna None para erro
    except Exception as e:
        logger.error(f"[NOTION] Erro obra: {e}")
        return None

def gerar_titulo(obra_nome, obra_id):
    try:
        response = query_notion_database(NOTION_DB_APONTAMENTOS, {"property": "Obras", "relation": {"contains": obra_id}})
        total = len(response.get("results", [])) + 1
        return f"{obra_nome} - {total:03d}"
    except Exception as e:
        logger.error(f"[TÍTULO] Erro: {e}")
        return f"{obra_nome} - 001"

@app.route("/", methods=["POST"])
def kobo_webhook():
    data = request.get_json(force=True)
    print("Payload recebido do Kobo:", data)
    return jsonify({"status": "ok"}), 200



def receber_dados():
    try:
        logger.info(f"[REQUEST] Headers: {dict(request.headers)}")
        logger.info(f"[REQUEST] Body: {request.get_data()}")

        dados = request.get_json()
        if not dados:
            return jsonify({"erro": "Dados JSON ausentes"}), 400

        auth_header = request.headers.get('Authorization')
        token = auth_header.replace('Bearer ', '', 1) if auth_header else dados.get('token')
        if token and token != KOBO_TOKEN:
            return jsonify({"erro": "Token inválido"}), 401

        obra = dados.get("obra", "")
        if not obra:
            return jsonify({"erro": "Campo obra obrigatório"}), 400

        obra_id = obter_obra_id(obra)
        if not obra_id:
            return jsonify({"erro": "Obra não encontrada"}), 400  # Erro se obra não existir

        titulo = gerar_titulo(obra, obra_id)
        usuario_id = obter_usuario_por_login(dados.get("_submitted_by", ""))

        # Processamento de múltiplas fotos usando _attachments
        links_fotos = []
        attachments = dados.get("_attachments", [])
        for attachment in attachments:
            filename = attachment.get("filename")
            download_url = attachment.get("download_url")
            if filename and download_url:
                link = upload_para_drive(filename, download_url)
                if link:
                    links_fotos.append(f"Foto: {link}")

        propriedades = {
            "Título": {"title": [{"text": {"content": titulo}}]},
            "Obras": {"relation": [{"id": obra_id}]},
            "Localização": {"rich_text": [{"text": {"content": dados.get("localizacao", "")}}]},
            "Apontamentos": {"rich_text": [{"text": {"content": dados.get("apontamento", "")}}]},
            "Status": {"select": {"name": dados.get("status", "")}},
            "Data de Criação": {"date": {"start": dados.get("_submission_time", "")}},
            "UUID": {"rich_text": [{"text": {"content": dados.get("_uuid", "")}}]}
        }
        if usuario_id:
            propriedades["Resp"] = {"relation": [{"id": usuario_id}]}
        if links_fotos:
            propriedades["Fotos"] = {"rich_text": [{"text": {"content": "\n".join(links_fotos)}}]}
            propriedades["Docs"] = {"rich_text": [{"text": {"content": "\n".join(links_fotos)}}]}

        pagina = notion.pages.create(
            parent={"database_id": NOTION_DB_APONTAMENTOS},
            properties=propriedades
        )
        logger.info(f"[NOTION] Página criada: {pagina['id']}")
        return jsonify({"status": "OK", "notion_page": pagina['id']}), 200

    except Exception as e:
        logger.exception("[ERROR] Erro geral")
        return jsonify({"erro": str(e)}), 500

if __name__ == "__main__":
    app.run(debug=True, host='0.0.0.0', port=10000)
