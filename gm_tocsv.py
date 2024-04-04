import os.path
import base64
import re
import json
import csv
import datetime
import sys
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# 必要な機能(アクセス内容)に応じて設定
# scopeを変更する場合は一度jsonファイルを削除する必要がある
SCOPES = ['https://www.googleapis.com/auth/gmail.modify']
TOKEN_PATH = r'credential\token.json'
CLIENT_PATH = r'credential\gmail_credentials_fa.json'
LABEL_NAME = 'csv出力済み'
QUERY = f"subject:[SBI証券]約定通知／注文番号 from: newer_than:1Y -label:{LABEL_NAME}"

def certify():
    """GmailAPIを呼び出すための認証を実行"""
    creds = None
    if os.path.exists(TOKEN_PATH):
        creds = Credentials.from_authorized_user_file(TOKEN_PATH, scopes=SCOPES)

    # Tokenのリフレッシュまたは、再認証(ユーザログイン)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(CLIENT_PATH, scopes=SCOPES)
            creds = flow.run_local_server(post=0)
        with open(TOKEN_PATH, 'w') as token:
            token.write(creds.to_json())
    return creds

def search_message(service, query):
    """検索条件に一致する thread ID / message data を取得"""
    match_messages = service.users().messages().list(userId='me', q=query).execute()
    if match_messages['resultSizeEstimate'] == 0: sys.exit("検索条件にマッチするメールが見つかりませんでした")
    thread_ids = [i['threadId'] for i in match_messages['messages']]
    message_all = []

    for message in match_messages['messages']:
        message_all.append(
            service.users().messages().get(userId='me', id=message['id']).execute()
            )
    return thread_ids, message_all

def adjust_message(message_all):
    """messageデータから本文のみを抽出し、デコード"""
    message_bodys = []
    plain_bodys = []

    # 本文抽出
    for mes_body in message_all:
        message_bodys.append(
            mes_body['payload'].get('parts')[0]['body']['data']
            )
    # 本文デコード
    for mes in message_bodys:
        mes = mes.replace('-', '+').replace('_', '/')
        plain_bodys.append(base64.b64decode(mes).decode('utf-8'))
    return plain_bodys

def pickdata(plain_bodys):
    """message本文から必要な項目値を取得"""
    export_list = [
            ['約定日時', '注文番号', '取引種別', '銘柄名', '銘柄コード', '取引所', '株数', '約定価格'],
            ]
    for pl_body in plain_bodys:
        transaction_time = re.search(r'約定日時(.+)', pl_body).groups()[0].strip()
        order_number = re.search(r'注文番号[^:](.+)', pl_body).groups()[0].strip()
        transaction_type = re.search(r'取引種別(.+)', pl_body).groups()[0].strip()
        security_name = re.search(r'銘柄名（銘柄コード）(.*)\(.*\)', pl_body).groups()[0].strip()
        security_code = re.search(r'銘柄名（銘柄コード）.*\((.*)\)', pl_body).groups()[0].strip()
        stock_exchange = re.search(r'取引所(.*)', pl_body).groups()[0].strip()
        quantity_stocks = re.search(r'株数(.*)', pl_body).groups()[0].strip()
        transaction_price = re.search(r'約定価格(.*)', pl_body).groups()[0].strip()

        export_list.append([
            transaction_time, order_number, transaction_type, security_name, security_code,
            stock_exchange, quantity_stocks, transaction_price
            ])
    return export_list

def get_time_forcsv():
    """出力するcsvのファイル名に加える現在時間を取得"""
    dt_now = datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=9),'JST'))
    dt_str = dt_now.strftime('%Y%m%d%H%M%S')
    return dt_str

def export_csv(export_list, dt_str):
    """csvファイルを作成"""
    with open(f'gmail_{dt_str}.csv', 'w', newline="") as f:
        writer = csv.writer(f)
        for row_dt in export_list:
            writer.writerow(row_dt)

def arrangement_label(service):
    """threadへセットするラベルを設定"""
    label_body = {
        'name': LABEL_NAME,
        'labelListVisibility': 'labelShow',
        'messageListVisibility': 'show',
        }
    # 付与するラベルが未作成の場合は新規作成
    userlabels = service.users().labels().list(userId='me').execute()
    label_list = [i['name'] for i in userlabels['labels']]
    if LABEL_NAME not in label_list:
        new_label = service.users().labels().create(userId='me', body=label_body).execute()
        label_id = new_label['id']
    else:
        label_id = next(dic for dic in userlabels['labels'] if dic['name'] == LABEL_NAME)['id']

    return label_id

def set_label(service, label_id, thread_ids, dt_str):
    if os.path.exists(f'gmail_{dt_str}.csv'):
        thread_label_body = {'addLabelIds':[label_id]}
        for thread_id in thread_ids:
            service.users().threads().modify(userId='me', id=thread_id, body=thread_label_body).execute()
        print('csvファイルの出力が完了 / 出力したメールにラベルを付与済み')

def main():
    creds = certify()

    try:
        # Gmail APIの呼び出し
        service = build('gmail', 'v1', credentials=creds)
        thread_ids, message_all = search_message(service, QUERY)
        plain_bodys = adjust_message(message_all)
        export_list = pickdata(plain_bodys)
        dt_str = get_time_forcsv()

        # csvの書き出し
        export_csv(export_list, dt_str)
        label_id = arrangement_label(service)
        set_label(service, label_id, thread_ids, dt_str)

    except HttpError as error:
        print(f"【An error occurred】: {error}")

if __name__ == '__main__':
    main()