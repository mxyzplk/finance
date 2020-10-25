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
import datetime
import requests
import os
import zipfile
import plotly as plt
import numpy as np

class data():
    def __init__(self, t1, t2, workdir):  #day/month/year
        self.dates = data.tdata(t1, t2)
        self.filenames = data.download(self.dates, workdir)
        self.data = []
        self.data = data.readdata(self.filenames, workdir)


    @classmethod
    def candlesticks():
        pass

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
        return d

    @staticmethod
    def indopc_dic(indopc):

        indopc_lib = [["US$", "CORREÇÃO PELA TAXA DO DÓLAR"],
        ["TJLP", "CORREÇÃO PELA TJLP"],
        ["TR", "CORREÇÃO PELA TR"],
        ["IPCR", "CORREÇÃO PELO IPCR"],
        ["SWA", "OPÇÕES DE TROCA - SWOPTIONS"],
        ["ÍNDICES", "(PONTOS) OPÇÕES REFERENCIADAS EM PONTOS DE ÍNDICE"],
        ["US$", "(PROTEGIDAS) CORREÇÃO PELA TAXA DO DÓLAR - OPÇÕES PROTEGIDAS"],
        ["IGPM", "(PROTEGIDA) CORREÇÃO PELO IGP-M - OPÇÕES PROTEGIDAS"],
        ["URV", "CORREÇÃO PELA URV"]]

        return [indopc_lib[int(indopc)][0],indopc_lib[int(indopc)][1]]
        
    
    @staticmethod
    def codbdi_dic(codbdi):
    
        codbdi_lib = {
        "02": "LOTE PADRÃO",
        "06": "CONCORDATÁRIAS",
        "10": "DIREITOS E RECIBOS",
        "12": "FUNDOS IMOBILIÁRIOS",
        "14": "CERTIFIC. INVESTIMENTO / DEBÊNTURES / TÍTULOS DIVIDA PÚBLICA",
        "18": "OBRIGAÇÕES",
        "22": "BÔNUS (PRIVADOS)",
        "26": "APÓLICES / BÔNUS / TÍTULOS PÚBLICOS",
        "32": "EXERCÍCIO DE OPÇÕES DE COMPRA DE ÍNDICE",
        "33": "EXERCÍCIO DE OPÇÕES DE VENDA DE ÍNDICE",
        "38": "EXERCÍCIO DE OPÇÕES DE COMPRA",
        "42": "EXERCÍCIO DE OPÇÕES DE VENDA",
        "46": "LEILÃO DE TÍTULOS NÃO COTADOS",
        "48": "LEILÃO DE PRIVATIZAÇÃO",
        "50": "LEILÃO",
        "51": "LEILÃO FINOR",
        "52": "LEILÃO FINAM",
        "53": "LEILÃO FISET",
        "54": "LEILÃO DE AÇÕES EM MORA",
        "56": "VENDAS POR ALVARÁ JUDICIAL",
        "58": "OUTROS",
        "60": "PERMUTA POR AÇÕES",
        "61": "META",
        "62": "TERMO",
        "66": "DEBÊNTURES COM DATA DE VENCIMENTO ATÉ 3 ANOS",
        "68": "DEBÊNTURES COM DATA DE VENCIMENTO MAIOR QUE 3 ANOS",
        "70": "FUTURO COM MOVIMENTAÇÃO CONTÍNUA",
        "71": "FUTURO COM RETENÇÃO DE GANHO",
        "74": "OPÇÕES DE COMPRA DE ÍNDICES",
        "75": "OPÇÕES DE VENDA DE ÍNDICES",
        "78": "OPÇÕES DE COMPRA",
        "82": "OPÇÕES DE VENDA",
        "83": "DEBÊNTURES E NOTAS PROMISSÓRIAS",
        "96": "FRACIONÁRIO",
        "99": "TOTAL GERAL"
        }
        res = codbdi_lib[codbdi]
        return res
    
    @staticmethod
    def codneg_dic(codneg):
        codneg_lib = {
        "ON": "AÇÕES ORDINÁRIAS NOMINATIVAS",
        "PNA": "AÇÕES PREFERENCIAIS NOMINATIVAS CLASSE A",
        "PNB": "AÇÕES PREFERENCIAIS NOMINATIVAS CLASSE B",
        "PNC": "AÇÕES PREFERENCIAIS NOMINATIVAS CLASSE C",
        "PND": "AÇÕES PREFERENCIAIS NOMINATIVAS CLASSE D",
        "PNE": "AÇÕES PREFERENCIAIS NOMINATIVAS CLASSE",
        "PNF": "AÇÕES PREFERENCIAIS NOMINATIVAS CLASSE F",
        "PNG": "AÇÕES PREFERENCIAIS NOMINATIVAS CLASSE G",
        "PNH": "AÇÕES PREFERENCIAIS NOMINATIVAS CLASSE H",
        "PN": " AÇÕES PREFERENCIAIS NOMINATIVAS",
        "PNV":"AÇÕES PREFERENCIAIS NOMINATIVAS COM DIREITO A VOTO",
        "OR": "AÇÕES ORDINÁRIAS NOMINATIVAS RESGATÁVEIS",
        "PRA": "AÇÕES PREFERENCIAIS NOMINATIVAS CLASSE A RESGATÁVEIS",
        "PRB": "AÇÕES PREFERENCIAIS NOMINATIVAS CLASSE B RESGATÁVEIS",
        "PRC": "AÇÕES PREFERENCIAIS NOMINATIVAS CLASSE C RESGATÁVEIS",
        "PRD": "AÇÕES PREFERENCIAIS NOMINATIVAS CLASSE D RESGATÁVEIS",
        "PRE": "AÇÕES PREFERENCIAIS NOMINATIVAS CLASSE E RESGATÁVEIS",
        "PRF": "AÇÕES PREFERENCIAIS NOMINATIVAS CLASSE F RESGATÁVEIS",
        "PRG": "AÇÕES PREFERENCIAIS NOMINATIVAS CLASSE G RESGATÁVEIS",
        "PRH": "AÇÕES PREFERENCIAIS NOMINATIVAS CLASSE H RESGATÁVEIS",
        "PNR": "AÇÕES PREFERENCIAIS NOMINATIVAS RESGATÁVEIS",
        "PRV": "AÇÕES PREFERENCIAIS NOMINATIVAS COM DIREITO A VOTO RESG",
        "ON P": "AÇÕES ORDINÁRIAS NOMINATIVAS COM DIREITOS DIFERENCIADOS",
        "PNA P": "AÇÕES PREFERENCIAIS NOMINATIVAS CLASSE A C/ DIREITOS DIFER",
        "PNB P": "AÇÕES PREFERENCIAIS NOMINATIVAS CLASSE B C/ DIREITOS DIFER",
        "PNC P": "AÇÕES PREFERENCIAIS NOMINATIVAS CLASSE C C/ DIREITOS DIFER",
        "PND P": "AÇÕES PREFERENCIAIS NOMINATIVAS CLASSE D C/ DIREITOS DIFER",
        "PNE P": "AÇÕES PREFERENCIAIS NOMINATIVAS CLASSE E C/ DIREITOS DIFER",
        "PNF P": "AÇÕES PREFERENCIAIS NOMINATIVAS CLASSE F C/ DIREITOS DIFER",
        "PNG P": "AÇÕES PREFERENCIAIS NOMINATIVAS CLASSE G C/ DIREITOS DIFER",
        "PNH P": "AÇÕES PREFERENCIAIS NOMINATIVAS CLASSE H C/ DIREITOS DIFER",
        "PN P": "AÇÕES PREFERENCIAIS NOMINATIVAS COM DIREITOS DIFERENCIADOS",
        "PNV P": "AÇÕES PREFERENCIAIS NOMINATIVAS CLASSE V C/ DIREITOS DIFER",
        "BDR": "BDR",
        "UNT": "CERTIFICADO DE DEPOSITO DE AÇÕES - MISCELÂNEA",
        "CDA": "CERTIFICADO DE DEPOSITO DE AÇÕES ORDINÁRIAS",
        "CPA": "CERTIFICADOS DE POTENCIAL ADICIONAL DE CONSTRUÇÃO E OPERAÇÃO",
        "RON": "CESTA DE AÇÕES ORDINÁRIAS NOMINATIVAS",
        "R": "CESTA DE AÇÕES NOMINATIVAS",
        "C I": "FUNDO DE INVESTIMENTO",
        "DIR": "DIREITOS DE SUBSCRIÇÃO MISCELÂNEA (BÔNUS, DEBÊNTURES, ETC)",
        "DIR ORD": "DIREITOS DE SUBSCRIÇÃO EM AÇÕES ORDINÁRIAS",
        "DIR P/A": "DIREITOS DE SUBSCRIÇÃO EM AÇÕES PREFERENCIAIS CLASSE A",
        "DIR P/B": "DIREITOS DE SUBSCRIÇÃO EM AÇÕES PREFERENCIAIS CLASSE B",
        "DIR P/C": "DIREITOS DE SUBSCRIÇÃO EM AÇÕES PREFERENCIAIS CLASSE C",
        "DIR P/D": "DIREITOS DE SUBSCRIÇÃO EM AÇÕES PREFERENCIAIS CLASSE D",
        "DIR P/E": "DIREITOS DE SUBSCRIÇÃO EM AÇÕES PREFERENCIAIS CLASSE E",
        "DIR P/F": "DIREITOS DE SUBSCRIÇÃO EM AÇÕES PREFERENCIAIS CLASSE F",
        "DIR P/G": "DIREITOS DE SUBSCRIÇÃO EM AÇÕES PREFERENCIAIS CLASSE G",
        "DIR P/H": "DIREITOS DE SUBSCRIÇÃO EM AÇÕES PREFERENCIAIS CLASSE H",
        "DIR PRE": "DIREITOS DE SUBSCRIÇÃO EM AÇÕES PREFERENCIAIS",
        "PRA REC": "RECIBO DE SUBSCRIÇÃO EM AÇÕES RESGATÁVEIS PREF. CLASSE A",
        "PRB REC": "RECIBO DE SUBSCRIÇÃO EM AÇÕES RESGATÁVEIS PREF. CLASSE B",
        "PRC REC": "RECIBO DE SUBSCRIÇÃO EM AÇÕES RESGATÁVEIS PREF. CLASSE C",
        "M1 REC": "RECIBO DE SUBSCRIÇÃO DE MISCELÂNEAS",
        "DIR PRA": "DIREITOS DE SUBSCRIÇÃO EM AÇÕES RESGATÁVEIS PREF. CLASSE A",
        "DIR PRB": "DIREITOS DE SUBSCRIÇÃO EM AÇÕES RESGATÁVEIS PREF. CLASSE B",
        "DIR PRC": "DIREITOS DE SUBSCRIÇÃO EM AÇÕES RESGATÁVEIS PREF. CLASSE C",
        "LFT": "LETRA FINANCEIRA DO TESOURO",
        "BNS ORD": "BÔNUS DE SUBSCRIÇÃO EM AÇÕES ORDINÁRIAS",
        "BNS P/A": "BÔNUS DE SUBSCRIÇÃO EM AÇÕES PREFERENCIAIS CLASSE A",
        "BNS P/B": "BÔNUS DE SUBSCRIÇÃO EM AÇÕES PREFERENCIAIS CLASSE B",
        "BNS P/C": "BÔNUS DE SUBSCRIÇÃO EM AÇÕES PREFERENCIAIS CLASSE C",
        "BNS P/D": "BÔNUS DE SUBSCRIÇÃO EM AÇÕES PREFERENCIAIS CLASSE D",
        "BNS P/E": "BÔNUS DE SUBSCRIÇÃO EM AÇÕES PREFERENCIAIS CLASSE E",
        "BNS P/F": "BÔNUS DE SUBSCRIÇÃO EM AÇÕES PREFERENCIAIS CLASSE F",
        "BNS P/G": "BÔNUS DE SUBSCRIÇÃO EM AÇÕES PREFERENCIAIS CLASSE G",
        "BNS P/H": "BÔNUS DE SUBSCRIÇÃO EM AÇÕES PREFERENCIAIS CLASSE H",
        "BNS PRE": "BÔNUS DE SUBSCRIÇÃO EM AÇÕES PREFERENCIAIS",
        "PCD POSIÇÃO": "CONSOLIDADA DA DIVIDA",
        "UP": "PRECATÓRIO",
        "REC": "RECIBO DE SUBSCRIÇÃO MISCELÂNEA",
        "ON REC": "RECIBO DE SUBSCRIÇÃO EM AÇÕES ORDINÁRIAS",
        "PNA REC": "RECIBO DE SUBSCRIÇÃO EM AÇÕES PREFERENCIAIS CLASSE A",
        "PNB REC": "RECIBO DE SUBSCRIÇÃO EM AÇÕES PREFERENCIAIS CLASSE B",
        "PNC REC": "RECIBO DE SUBSCRIÇÃO EM AÇÕES PREFERENCIAIS CLASSE C",
        "PND REC ": "RECIBO DE SUBSCRIÇÃO EM AÇÕES PREFERENCIAIS CLASSE D",
        "PNE REC": "RECIBO DE SUBSCRIÇÃO EM AÇÕES PREFERENCIAIS CLASSE E",
        "PNF REC": "RECIBO DE SUBSCRIÇÃO EM AÇÕES PREFERENCIAIS CLASSE F",
        "PNG REC": "RECIBO DE SUBSCRIÇÃO EM AÇÕES PREFERENCIAIS CLASSE G",
        "PNH REC": "RECIBO DE SUBSCRIÇÃO EM AÇÕES PREFERENCIAIS CLASSE H",
        "PN REC": "RECIBO DE SUBSCRIÇÃO EM AÇÕES PREFERENCIAIS",
        "PNV REC": "RECIBO DE SUBSCRIÇÃO EM AÇÕES PREFERENCIAIS C/ DIREITO VOTO",
        "WRT": "WARRANTS DE DEBÊNTURES",
        "OR P": "AÇÕES ORDINÁRIAS NOMINATIVAS RESGATÁVEIS C/ DIREITOS DIF",
        "PRA P": "AÇÕES PREFERENCIAIS NOMINATIVAS CLASSE 'A' RESG. C/ DIR.DIF",
        "PRB P": "AÇÕES PREFERENCIAIS NOMINATIVAS CLASSE 'B' RESG. C/ DIR.DIF",
        "PRC P": "AÇÕES PREFERENCIAIS NOMINATIVAS CLASSE 'C' RESG. C/ DIR.DIF",
        "PRD P": "AÇÕES PREFERENCIAIS NOMINATIVAS CLASSE 'D' RESG. C/ DIR.DIF",
        "PRE P": "AÇÕES PREFERENCIAIS NOMINATIVAS CLASSE 'E' RESG. C/ DIR.DIF",
        "PRF P": "AÇÕES PREFERENCIAIS NOMINATIVAS CLASSE 'F' RESG. C/ DIR.DIF",
        "PRG P": "AÇÕES PREFERENCIAIS NOMINATIVAS CLASSE 'G' RESG. C/ DIR.DIF",
        "PRH P": "AÇÕES PREFERENCIAIS NOMINATIVAS CLASSE 'H' RESG. C/ DIR.DIF",
        "PR P": "AÇÕES PREFERENCIAIS NOMINATIVAS RESGATÁVEIS C/ DIREITOS DIF",
        "PRV P": "AÇÕES PREFERENCIAIS NOMINATIVAS RESG. C/ DIR.DIF. E DIR.VOTO"
        }
        res = codneg_lib[codneg]
        return res

    @staticmethod
    def tpmerc_dic(tpmerc):
        tpmerc_lib = {
        "010": "VISTA",
        "012": "EXERCÍCIO DE OPÇÕES DE COMPRA",
        "013": "EXERCÍCIO DE OPÇÕES DE VENDA",
        "017": "LEILÃO",
        "020": "FRACIONÁRIO",
        "030": "TERMO",
        "050": "FUTURO COM RETENÇÃO DE GANHO",
        "060": "FUTURO COM MOVIMENTAÇÃO CONTÍNUA",
        "070": "OPÇÕES DE COMPRA",
        "080": "OPÇÕES DE VENDA"
        }
        res = tpmerc_lib[tpmerc]
        return res


    @staticmethod
    def splitline(line):
        tipreg = line[0:2]
        date   = line[2:10]
        codbdi = line[10:12]
        codneg = line[12:24]
        tpmerc = line[24:27]
        nomres = line[27:39]
        especi = line[39:49]
        prazot = line[49:52]
        modref = line[52:56]
        preabe = line[56:69]
        premax = line[69:82]
        premin = line[82:95]
        premed = line[95:108]
        preult = line[108:121]
        preofc = line[121:134]
        preofv = line[134:147]
        totneg = line[147:152]
        quatot = line[152:170]
        voltot = line[170:188]
        preexe = line[188:201]
        indopc = line[201:202]
        datven = line[202:210]
        fatcot = line[210:217]
        ptoexe = line[217:230]
        codisi = line[230:242]
        dismes = line[242:245]
        
        aaaa = int(date[2:6])
        mm = int(date[6:8])
        dd = int(date[8:10])
        newdate = datetime.date(aaaa,mm,dd)

        d = [tipreg,      #0
        codbdi,           #1
        codneg,           #2           
        tpmerc,           #3
        nomres,           #4
        especi,           #5
        prazot,           #6
        modref,           #7
        preabe,           #8
        premax,           #9
        premin,           #10
        premed,           #11
        preult,           #12
        preofc,           #13
        preofv,           #14
        totneg,           #15
        quatot,           #16
        voltot,           #17
        preexe,           #18
        indopc,           #19
        datven,           #20
        fatcot,           #21
        ptoexe,           #22
        codisi,           #23
        dismes,           #24
        newdate]          #25

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
                #print("Downloading %s" %dest)
            else:
                pass
                #print("File %s already exists" %dest)
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
                newdate = datetime.date(yr,mon,day)
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
