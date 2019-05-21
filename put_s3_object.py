#!/usr/bin/env python3

import os
import sys
import base64
from argparse import ArgumentParser
from boto3 import Session
import pickle
import re
import glob
from pathlib import Path
import pandas

############################################################
# 定数
############################################################
# ユーザプールID、S3バケット名を参照する設定ファイルへの相対パス.
conf_path = '../common/conf/dependencies_{}.ini'
# デフォルトで参照するs3の複合化キー情報（2019/05/20時点）
put_s3_enc_ptns_tsv = 'put_s3_enc_ptns.tsv'


############################################################
# プログラム引数のパーサー構築
############################################################
parser = ArgumentParser(description='作業ディレクトリのファイルをS3にアップロードする.')
parser.add_argument('-c', '--profile', help='プロファイル. 未指定の場合はデフォルトプロファイルを参照する.', action='store', type=str, required=False)
parser.add_argument('-e', '--env', help='環境名', action='store', type=str, required=False)
parser.add_argument('-p', '--user-pool-id', help='ユーザプールID', action='store', type=str, required=False)
parser.add_argument('-u', '--username', help='ユーザ名（Email）', action='store', type=str, required=True)
parser.add_argument('-b', '--bucket', action='store', help='S3バケット名', type=str, required=False)
parser.add_argument('-s', '--src', help='作業ディレクトリ内のアップロード対象パス', action='store', type=str, required=True)
parser.add_argument('-d', '--dst', help = 'アップロード先のS3オブジェクトキー', action = 'store', type=str, required=True)

############################################################
# 関数定義
############################################################
def show_error(msg):
    """
    プログラム引数のヘルプ、エラーメッセージを表示する.
    :param msg: 表示するメッセージ.
    :return: なし.
    """
    stderr = sys.stderr
    parser.print_help(file=stderr)
    print('', file=stderr)
    parser.exit(message=msg)


def get_kms_data_key(session, user_pool_id, username):
    """
    指定したユーザプールから指定したユーザ情報を取得する.
    取得したユーザ情報には暗号化／復号化キーが含まれる.P
    :param session: AWSセッション.
    :param user_pool_id: ユーザプールID.
    :param username: ユーザ名（Email）.
    :return: ユーザ情報.
    """
    cognito = session.client('cognito-idp')
    return cognito.list_users(
        UserPoolId=user_pool_id,
        AttributesToGet=[
            'custom:user_kms_data_key',
            'custom:corp_kms_data_key',
        ],
        Filter='username = "{}"'.format(username)
    )['Users']

def make_param(bucket, load_path, s3_key, key = None):

    data = open(load_path, 'rb').read()
    s3_param = {
            'Bucket': bucket,
            'Key': s3_key,
            'Body': data
    }
    # 複合化キー有無の確認
    if key:
        add_param = {
            'SSECustomerKey' : key,
            'SSECustomerAlgorithm':'AES256'
            }
        s3_param.update(add_param)

    return s3_param

def match_enc_ptns(file_name):

    df = pandas.read_csv(put_s3_enc_ptns_tsv, sep = '\t', encoding = 'utf-8')

    for i, row in df.iterrows():
        ptn = re.compile(row['パターン'])
        score = int(row['値'])
        if re.match(ptn, os.path.basename(file_name)):
            return score


############################################################
# エントリポイント
############################################################
if __name__ == '__main__':

    ########################################
    # プログラム引数を解析
    ########################################
    args = parser.parse_args()
    if args.env is not None:
        if args.user_pool_id is not None or args.bucket is not None:
            show_error(msg='プログラム引数エラー: envを指定した場合は、user-pool-idとbucketを指定することはできません.')
        else:
            for p in ['common', 'hrtech_common']:
                sys.path.append(os.path.abspath(os.path.join('../{}'.format(p))))
            from singletons import ini
            ini.read(conf_path.format(args.env))
            args.user_pool_id = ini.get('aws', 'user_pool_id')
            args.bucket = ini.get('aws', 'bucket')
    else:
        if args.user_pool_id is None or args.bucket is None:
            show_error(msg='プログラム引数エラー: envを指定しない場合は、user-pool-idとbucketは必須です.')
    
    aws_session = Session(profile_name=args.profile)

    ########################################
    # ユーザの暗号化／復号化キーを取得
    ########################################
    users = get_kms_data_key(session=aws_session, user_pool_id=args.user_pool_id, username=args.username)
    if len(users) < 1:
        print('{} NOT Found.'.format(args.username))
        exit(1)

    corp_kms_data_key = users[0]['Attributes'][0]['Value']
    user_kms_data_key = users[0]['Attributes'][1]['Value']
    kms = aws_session.client('kms')
    user_kms_key = kms.decrypt(CiphertextBlob=base64.b64decode(user_kms_data_key))['Plaintext']
    corp_kms_key = kms.decrypt(CiphertextBlob=base64.b64decode(corp_kms_data_key))['Plaintext']

    ########################################
    # アップロード対象ファイルパスの取得
    ########################################
    # --srcと--dstをディレクトリとファイル名に分割する
    src_dir, src_file = os.path.split(args.src)
    dst_dir, dst_file = os.path.split(args.dst)
    
    #アップロード対象ファイルが存在しな場合、エラー
    if not os.path.exists(args.src):
        print('{} NOT Found.'.format(args.src))
        exit(1)
  
    if src_file and os.path.isfile(src_file):
        put_list = [os.path.join(args.src)]
    elif os.path.isdir(args.src):
        # 指定先ディレクトリ内のファイルとフォルダの一覧を取得
        path = Path(args.src)
        find_path = list(path.glob("*"))
        find_path = list(path.glob("**/*"))

        # 一覧からディレクトリパス以外を格納
        put_list = [str(f).replace('\\', '/') for f in find_path if os.path.isfile(f)]
    else:
        
   

    ##################################################
    # アップロード対象の暗号化情報を取得し、各リストに格納
    ##################################################

    # 暗号化されていないパスの格納用
    not_encrypted = []
    # user_kmsキーで暗号化されているパス格納用
    encrypted_with_user_kms = []
    # corp_kmsキーで暗号化されているパス格納用
    encrypted_with_corp_kms = []
    
    for item in put_list:
        print('item', item)
        enc_pattern = match_enc_ptns(item)
        if enc_pattern == 1:
            encrypted_with_user_kms.append(item)
        elif enc_pattern == 2:
            encrypted_with_corp_kms.append(item)
        elif enc_pattern == 0:
            not_encrypted.append(item)
        else:
            print('{0} NOT Found IN {1}.'.format(item, put_s3_enc_ptns_tsv))
            exit(1)

    # #####################################################
    # # 暗号化情報別に対象ファイルをS3にファイルをアップロード
    # #####################################################
    
    s3 = aws_session.client('s3')
    
    if encrypted_with_user_kms:
        for file in encrypted_with_user_kms:
            print('path_split', args.src.split('/'))
            print('path_split', len(file.split('/')))

            s3_key = file.replace(len(file.split))
    #         s3_param = make_param(args.bucket, file, s3_key, user_kms_key)
    #         #s3.put_object(**s3_param)
    #         print('Upload File: {0} To {1}'.format(file, ))
    # elif not_encrypted:
    #     for file in not_encrypted:
    #         s3_param = make_param(args.bucket, file, s3_key)
    #         #s3.put_object(**s3_param)
    #         print('Upload File: {0} To {1}'.format(file, ))
    # elif encrypted_with_corp_kms:
    #     for file in encrypted_with_corp_kms:
    #         s3_param = make_param(args.bucket, file, s3_key, corp_kms_key)
    #         #s3.put_object(**s3_param)
    #         print('Upload File: {0} To {1}'.format(file, ))
