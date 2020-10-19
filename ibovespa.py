# This project aims to collect and use stocks historical data from the Bovespa Index, 
# he benchmark index of about 70 stocks[1] that are traded on the 
# B3 (Brasil Bolsa Balcão), which account for the majority of trading and 
# market capitalization in the Brazilian stock market.
#
# Given a certain period of time, the code searchs for each day's data and stores
# it. The code also validates which data file is valid for use and store the data
# in a list.
# 
# WIP 
#
import numpy as np
import datetime
import requests
import os
import zipfile
import mysql.connector as mysql

class data():
    def __init__(self, t1, t2, workdir):  #day/month/year
        self.dates = data.tdata(t1, t2)
        self.filenames = data.download(self.dates, workdir)
        self.data = data.readdata(self.filenames, workdir)
        #db = mysql.connect(
        #    host = "localhost",
        #    user = "root",
        #    passwd = "dbms"
        #)
        #cursor = db.cursor()
        #cursor.execute("CREATE DATABASE datacamp")
        #databases = cursor.fetchall()
        #print(databases)

    @classmethod
    def readdata(cls, filenames, workdir):
        nfiles = len(filenames)
        d = []
        for i in range(nfiles):
            if filenames[i][1] == "Available":
                zip = zipfile.ZipFile(filenames[i][0])
                raw = filenames[i][0]
                raw = raw.replace("ZIP","TXT")
                try:
                    os.remove(workdir,raw)
                except:
                    pass
                zip.extractall(workdir)
                nlines = sum(1 for line in open(raw))
                f = open(raw, "r")
                for j in range(nlines):
                    line = f.readline()
                    if (j > 0):
                        d.append(data.splitline(line))
    
    @staticmethod
    def splitline(line):
        d = []
        tipo_de_pregao = line[0:2]
        codigo_bdi = line[10:12]
        codigo_negociacao = line[12:24]
        tipo_de_mercado = line[24:27]
        nome_empresa = line[27:39]
        especificao_papel = line[39:49]
        prazo_mercado_termo = line[49:52]
        moeda_ref = line[52:56]
        prec_abertura = line[56:69]
        prec_maximo = line[69:82]
        prec_minimo = line[82:95]
        prec_medio = line[95:108]
        prec_ult = line[108:121]
        preco_melhor_oferta_compra = line[121:134]
        preco_melhor_oferta_venda = line[134:147]
        num_negociacoes = line[147:152]
        num_titulos_negociados = line[152:170]
        vol_titulos_negociados = line[170:188]
        preco_exercicio = line[188:201]
        indicador_de_correcao = line[201:202]
        data_vencimento = line[202:210]
        fator_cotacao = line[210:217]
        preco_exercicio_pts = line[217:230]
        codigo_papel_isin = line[230:242]
        num_distribuicao_pappel = line[242:245]
        d.append([tipo_de_pregao,
                    codigo_bdi,
                    codigo_negociacao,
                    tipo_de_mercado,
                    nome_empresa,
                    especificao_papel,
                    prazo_mercado_termo,
                    moeda_ref,
                    prec_abertura,
                    prec_maximo,
                    prec_minimo,
                    prec_medio,
                    prec_ult,
                    preco_melhor_oferta_compra,
                    preco_melhor_oferta_venda,
                    num_negociacoes,
                    num_titulos_negociados,
                    vol_titulos_negociados,
                    preco_exercicio,
                    indicador_de_correcao,
                    data_vencimento,
                    fator_cotacao,
                    preco_exercicio_pts,
                    codigo_papel_isin,
                    num_distribuicao_pappel,
                    codigo_bdi,
                    codigo_negociacao,
                    tipo_de_mercado,
                    nome_empresa,
                    especificao_papel,
                    prazo_mercado_termo,
                    moeda_ref,
                    prec_abertura,
                    prec_maximo,
                    prec_minimo,
                    prec_medio,
                    prec_ult,
                    preco_melhor_oferta_compra,
                    preco_melhor_oferta_venda,
                    num_negociacoes,
                    num_titulos_negociados,
                    vol_titulos_negociados,
                    preco_exercicio,
                    indicador_de_correcao,
                    data_vencimento,
                    fator_cotacao,
                    preco_exercicio_pts,
                    codigo_papel_isin,
                    num_distribuicao_pappel]) 
        return d    
                


    @classmethod
    def download(cls, dates, workdir):
        os.makedirs(workdir,mode=0o777,exist_ok=True)
        #
        nfiles = len(dates)
        res = []
        for i in range(nfiles):
            day = int(dates[i][0])
            mon = int(dates[i][1])
            yr = int(dates[i][2])
            filename = "COTAHIST_D%02d%02d%02d.ZIP" % (day,mon,yr)
            #
            sourcelink = "http://bvmf.bmfbovespa.com.br/InstDados/SerHist/%s" %filename
            dest = "%s/%s" %(workdir, filename)
            #
            if os.path.exists(dest) == False:
                req = requests.get(sourcelink, allow_redirects=True)
                open(dest, 'wb').write(req.content)
                print("Downloading %s" %dest)
            else:
                print("File %s already exists" %dest)
            # check availability
            try:
                zip = zipfile.ZipFile(dest)
            except:
                res.append([dest,"Not Available"])
            else:
                res.append([dest,"Available"])
                zip.close()
        return res

    @classmethod
    def tdata (cls, t1, t2):  
        t1_l = []
        t2_l = []
        ti_l = []
        res  = []
        t1_l = t1.split("/")
        t2_l = t2.split("/")
        t1_l = list(map(int, t1_l))
        t2_l = list(map(int, t2_l))
        ti_l = t1_l
        fin = False
        while fin == False:
            day = int(ti_l[0])
            mon = int(ti_l[1])
            yr  = int(ti_l[2])
            #
            if all(i == j for (i, j) in list(zip(ti_l, t2_l))) == True:
                fin = True           
            try:
                newdate = datetime.datetime(yr,mon,day)
                res.append([day,mon,yr])
            except:
                continue
            finally:
                if (day == 31):
                    day = 1
                    mon = mon + 1
                else:
                    day = day + 1
                if (day == 1 and mon == 13):
                    mon = 1
                    yr = yr + 1
                ti_l = [day,mon,yr]
        return res
    #
a = data("01/01/2019","05/02/2019","/home/mxyz/git_workspace/web_server/dados")

            



