import re
import json
import locale
import logging
import smtplib
import pandas as pd
import numpy as np

import smtplib
from email import encoders
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime


locale.setlocale(locale.LC_ALL, '')


class ANALYTICS():

    recipients = [
        'wesleycarvalhop@gmail.com',
        'andre.jfmdm@gmail.com',
        'vitoredo1097@gmail.com'
    ]

    saving_path = None

    def get_save_path(self, file_path):
        return f'{file_path.split("dados")[0]}/relatorios/resumo_base'

    def _build_df_from_file(self, json_list):

        logging.info(
            "{} Carregando os dados.".format(
                datetime.today().strftime("%d-%m-%Y-%H:%M:%S")
            )
        )
        
        df = pd.json_normalize(json_list)

        rename_dic = { 
            "long_comment" : "comentario",
            "fipe_percent" : "fipe_perc",
            "unico_dono" : "unico_dono",
            "ipva_pago" : "ipva_pago",
            "aceita_troca" : "aceita_troca",
            "financiado" : "financiado",
            "licenciado" : "licenciado",
            "contains_leilao" : "contains_leilao",
            "specification.title" : "titulo",
            "specification.make.value" : "marca",
            "specification.model.value" : "modelo",
            "specification.version.value" : "versao",
            "specification.year_fabrication" : "ano",
            "specification.year_model" : "ano_modelo",
            "specification.odometer" : "km",
            "specification.transmission" : "transmissao",
            "specification.number_ports" : "num_portas",
            "specification.body_type" : "categoria",
            "specification.armored" : "blindado",
            "specification.color_primary" : "cor",
            "seller.seller_type" : "vendedor_tipo",
            "seller.city" : "vendedor_cidade",
            "seller.state" : "vendedor_estado",
            "seller.dealer_score" : "score_vendedor",
            "seller.car_delivery" : "car_delivery",
            "seller.troca_com_troco" : "troca_troco",
            "seller.exceeded_plan" : "exceeded_plan",
            "price" : "preco",
        }

        df = df.rename(columns=rename_dic)

        cols_select = [
            'titulo', 'marca', 'modelo', 'versao','ano', 'ano_modelo', 
            'km', 'transmissao', 'num_portas','categoria', 'blindado', 
            'cor', 'preco', 'fipe_perc', 'unico_dono', 'ipva_pago', 
            'aceita_troca', 'financiado', 'licenciado', 'contains_leilao',
            'vendedor_tipo', 'vendedor_cidade', 'vendedor_estado', 'score_vendedor',
            'car_delivery', 'troca_troco', 'comentario', 'combustivel','motor',
            'cilindrada', 'tracao','valvulas','cilindros_disposicao'
        ]

        # pra garantir que tem apenas as colunas que existem (que nao foram dropadas antes)
        cols_select = [item for item in cols_select if item in df.columns]

        df = df[cols_select]

        # ajustando dtypes
        df["ano"] = df["ano"].astype(int)

        df["ano_modelo"] = df["ano_modelo"].astype(int)

        df["num_portas"] = df["num_portas"].astype(int)

        return df

    def _fix_column_names(self, df_export_view):
        human_readable_columns = [
            'Marca','Modelo','Ano Modelo','Transmissão','Categoria','Blindado','Nº Portas','Cilindrada', 'Combustível', 'Estado',
            'Km Médio', 'Km Mínimo', 'Km Máximo', 'Km Mediana', 'Km Desvio Padrão', 'Nº de Carros Encontrados',
            'Média dos Preços', 'Preço Mínimo', 'Preço Máximo','Mediana dos Preços', 'Desvio Padrão dos Preços', '(%) Tabela FIPE Médio',
            '(%) Tabela FIPE Mínino', '(%) Tabela FIPE Máximo', '(%) Tabela FIPE Mediana','(%) Tabela FIPE Desvio Padrão' 
        ]

        df_export_view.columns = human_readable_columns

        columns_swap = [
            'Marca','Modelo','Ano Modelo','Transmissão','Categoria','Blindado','Nº Portas','Cilindrada', 'Combustível','Estado',
            'Nº de Carros Encontrados','Km Médio', 'Km Mínimo', 'Km Máximo', 'Km Mediana', 'Km Desvio Padrão', 
            'Média dos Preços', 'Preço Mínimo', 'Preço Máximo','Mediana dos Preços', 'Desvio Padrão dos Preços', '(%) Tabela FIPE Médio',
            '(%) Tabela FIPE Mínino', '(%) Tabela FIPE Máximo', '(%) Tabela FIPE Mediana','(%) Tabela FIPE Desvio Padrão' 
        ]
        
        df_export_view = df_export_view[columns_swap]

        return df_export_view
    
    def descriptive_statistics(self, json_list, file_path):
        
        logging.info(
            "{} Iniciando análise descrtiva da base.".format(
                datetime.today().strftime("%d-%m-%Y-%H:%M:%S")
            )
        )

        df = self._build_df_from_file(json_list)
        
        ##################################################
        

        marcas = df["marca"].unique().tolist()

        df_marcas = {marca: df[df["marca"] == marca] for marca in marcas}

        # não tá sendo usado...
        
#         modelos_describe = {marca : 
#             {modelo: df_marcas[marca].loc[df_marcas[marca]["modelo"] == modelo].describe() 
#                             for modelo in df_marcas[marca]["modelo"].unique().tolist()}
#                     for marca in marcas}

        
        # características que definem um carro ("RG do carro")
        rg = "marca, modelo, ano_modelo, transmissao, categoria, blindado, num_portas, cilindrada, combustivel, vendedor_estado".split(", ")

        marca_modelo_rg = {marca : 
            {modelo: df_marcas[marca][df_marcas[marca]["modelo"] == modelo].groupby(rg)[
                ["km", "preco", "fipe_perc"]].agg(["count", "mean","min","max",np.median,np.std]) for modelo in df_marcas[marca]["modelo"].unique().tolist()}
                    for marca in marcas}

        ##################################################

        df_export = pd.DataFrame()

        for marca in marca_modelo_rg.keys():
    
            for modelo in marca_modelo_rg[marca].keys():
        
                df_export = pd.concat([df_export, marca_modelo_rg[marca][modelo]])
            
        ##################################################

        df_export_view = df_export.copy()

        df_export_view = df_export_view.fillna("-")

        # formatando dados
        for col in df_export_view.columns:
    
            if col[0] == "preco":
    
                df_export_view.loc[:, 
                            col] = df_export_view.loc[:, 
                                            col].apply(lambda x: 
                                                       locale.currency(x, grouping=True) if type(x) == float
                                                       else (x if type(x) == int else "-"))
            else:
        
                df_export_view.loc[:, 
                            col] = df_export_view.loc[:, 
                                                    col].apply(lambda x:
                                                       locale.format_string("%.2f", x, grouping=True) if type(x) == float
                                                       else (x if type(x) == int else "-"))


        # flattening columns
        df_export_view.columns = ['_'.join(col).strip() for col in df_export_view.columns.values]

        # flatenning indexes
        df_export_view = df_export_view.reset_index()
        
        # ordenando
        df_export_view = df_export_view.sort_values(["marca", "modelo", "preco_count"])
        
        ##################################################
        
        # export
        data_hora = file_path.split("/")[-1].split("WEBMOTORS-")[-1].split(".json")[0]

        save_path = self.get_save_path(file_path)

        df_export_view.drop(['km_count','fipe_perc_count'], axis = 1, inplace = True)
        
        df_final = self._fix_column_names(df_export_view)

        df_final.to_excel(f"{save_path}/report_{data_hora}.xlsx", index=False)

        return f"{save_path}/report_{data_hora}.xlsx"

    def send_email(self, report_path):
        
        logging.info(
            "{} Enviando e-mails.".format(
                datetime.today().strftime("%d-%m-%Y-%H:%M:%S")
            )
        )

        file_name = report_path.split('/')[-1]

        msg = MIMEMultipart()

        email_body = open('email.html', 'r')
        message = email_body.read()

        address = 'marquinhos95.relampago@gmail.com'
        password = 'focus.speed.iamspeed.'

        msg['From'] = address
        msg['To'] = (', ').join(self.recipients)
        msg['Subject'] = 'Relatório de Scraping - WebMotors'

        msg.attach(MIMEText(message,'html'))
        
        part = MIMEBase('application', "octet-stream")
        part.set_payload(open(report_path, "rb").read())
        encoders.encode_base64(part)
        part.add_header('Content-Disposition', f'attachment; filename={file_name}')
        msg.attach(part)

        s = smtplib.SMTP(host = 'smtp.gmail.com', port = 587)
        s.starttls()
        s.login(address, password)

        s.send_message(msg)
        s.quit()