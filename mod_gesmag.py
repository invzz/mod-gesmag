#!/usr/bin/python
# -*- coding: utf-8 -*-

import mysql.connector
import datetime


class Materiale:

    def __init__(self):
        self.id = 0
        self.desc = ''
        self.um = ''
        self.val = 0
        self.qt = 0.0
        self.tot = 0
        pass

    def set(
        self,
        _id,
        desc,
        um,
        val,
        qt,
        ):

        self.id = _id
        self.desc = desc
        self.um = um
        self.qt = float(qt)
        self.val = val
        self.tot = self.val*self.qt
        pass

    def show(self):
        print  self.desc + ' ::: ' + str(self.um) + ' :::: QUANTITA ::::' + str(self.qt)
        pass

class Fornitore:

    def __init__(self):
        self.id = 0
        self.desc = ''
        pass

    def set(self, _id, desc):
        self.id = _id
        self.desc = desc
        pass

class Appalto:

    def __init__(self):
        self.id = 0
        self.desc = ''
        pass

    def set(self, _id, desc):
        self.id = _id
        self.desc = desc
        pass

class Commessa:

    def __init__(self):
        self.id = 0
        self.desc = ''
        pass

    def set(self, _id, desc):
        self.id = _id
        self.desc = desc
        pass

class Ddt:

    def __init__(self):
        self.id = 0
        self.datetime = ''
        self.filename = Materiale()
        self.slag = ''

        pass

    def set(
        self,
        _id,
        datetime,
        filename,
        slag,
        ):

        self.id = _id
        self.datetime = datetime
        self.filename = filename
        self.slag = slag
        pass

class Carico:

    def __init__(self):
        self.id = 0
        self.ddt = Ddt()
        self.datetime = ''
        self.materiale = Materiale()
        self.fornitore = Fornitore()
        self.commessa = Commessa()
        self.qt = 0
        self.caricato = 0
        pass

    def set(
        self,
        _id,
        ddt,
        fornitore,
        datetime,
        materiale,
        commessa,
        qt,
        caricato
        ):

        self.id = _id
        self.ddt = ddt
        self.fornitore = fornitore
        self.datetime = datetime
        self.materiale = materiale
        self.commessa = commessa
        self.qt = qt
        self.caricato = caricato
        pass

class Scarico:

    def __init__(self):
        self.id = 0
        self.datetime = ''
        self.materiale = Materiale()
        self.appalto = Appalto()
        self.commessa = Commessa()
        self.qt = 0
        self.scaricato = 0
        pass

    def set(
        self,
        _id,
        datetime,
        materiale,
        appalto,
        commessa,
        qt,
        scaricato
        ):

        self.id = _id
        self.datetime = datetime
        self.materiale = materiale
        self.appalto = appalto
        self.commessa = commessa
        self.qt = qt
        self.scaricato = scaricato
        pass

    def sql(self):
        query ="INSERT INTO `scarichi` (`scarichi_data`, `scarichi_id_materiale`, `scarichi_id_subappalto`, `scarichi_id_commessa`, `scarichi_qt`, `scarichi_scaricato`) VALUES (\'"+self.datetime+"\',"+str(self.materiale.id)+","+str(self.appalto.id)+","+str(self.commessa.id)+","+str(self.qt)+","+str(self.scaricato)+")"
        return query
    
    def string(self):
        string = self.datetime +"||"+str(self.materiale.desc)+"||"+str(self.appalto.desc)+"||"+str(self.commessa.desc)+"||"+str(self.qt)
        return string

class Gesmag:

    def __init__(self):

        self.materiali = list()
        self.fornitori = list()
        self.appalti = list()
        self.commesse = list()
        self.carichi = list()
        self.scarichi = list()
        self.db_config = self.dbconfig()
        print '[ gesmag ]: Set di parametri inizializzati...'
        self.cnx = mysql.connector.connect(**self.db_config)
        self.cursor = self.cnx.cursor()
        print "[ gesmag ]: Database Collegato...\t"
        self.getMateriali()
        self.getFornitori()
        self.getAppalti()
        self.getDdt()
        self.getCommesse()
        self.getCarichi()
        self.getScarichi()
        self.giacenza = self.getGiacenza()
        pass

    def dic(self,tipo):
        if tipo == 'materiale':
            l_id    = list()
            l_desc  = list()
            l_um    = list()
            l_val   = list()
            l_qt    = list()
            for mat in self.materiali:
                l_id.append(mat.id)
                l_desc.append(mat.desc)
                l_um.append(mat.um)
                l_val.append(mat.val)
                l_qt.append(mat.qt)
            dict_mat = {
                        'id':l_id,
                        'descrizione':l_desc,
                        'um':l_um,
                        'val':l_val,
                        'qt':l_qt
                    }
            return dict_mat                

    def update(self):
        self.getMateriali()
        self.getFornitori()
        self.getAppalti()
        self.getDdt()
        self.getCommesse()
        self.getCarichi()
        self.getScarichi()
        pass
        
    def printCarichi(self):
        if len(self.carichi) == 0:
            print '[ gesmag ] -[ print-carichi ] : nessuno scarico'
            return 0
        for carico in self.carichi:
            print 'TUTTI CARICHI PRESENTI A SISTEMA:\n'
            print carico.id, carico.fornitore.desc, \
                carico.materiale.desc, carico.datetime, \
                carico.commessa.desc
        return True

    def printScarichi(self):
        print 'TUTTI SCARICHI PRESENTI A SISTEMA:'
        if len(self.carichi) == 0:
            print '[ gesmag ] -[ print-carichi ] : nessuno scarico'
            return 0        
        for scarico in self.scarichi:
            print scarico.id, scarico.datetime, scarico.materiale.desc, scarico.materiale.um, scarico.qt, scarico.appalto.desc, scarico.commessa.desc
        return False

    def dbconfig(self):
        db_config = {
            'user': '',
            'password': '',
            'host': '',
            'database': '',
            'raise_on_warnings': True,
            }
        return db_config

    def getFloat(self,string):
        temp = ""
        float_= 0
        for char in string:
            if char != '.':
                temp = temp + char
        float_ = float(temp.split(",")[0]+'.'+ temp.split(",")[1])
        return float_

    def setMatCsv(self,filename):
        q="INSERT INTO `gesmag`.`materiale` (`id_materiale`, `desc_materiale`, `um_materiale`, `giacenza_materiale`, `val_materiale`) VALUES (%i, %r, %r, %f, %f);"
        queryList = list()
        with open(filename) as csv:
            for line in csv:
                string = []
                string = line.split(";")
                id_mat = int(string[0])
                desc_mat = string[1]
                um_mat = string[2]
                qt = self.getFloat(string[3])
                val = self.getFloat(string[4])
                #print qt , val
                queryList.append(q % (id_mat, desc_mat, um_mat, qt, val))
        
        for query in queryList:        
            self.cursor.execute(query)
            self.cnx.commit()
            print query
        return 1
        
    def getMateriali(self):
        self.materiali = list()
        query = 'select * from materiale'
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        c = 0
        for line in rows:
            mat = Materiale()
            mat.set(line[0], line[1], line[2], line[4], line[3])
            c += 1
            self.materiali.append(mat)
        return 1

    def getFornitori(self):
        self.fornitori = list()
        query = 'select * from fornitori'
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        self.cnx.commit()
        c = 0
        for line in rows:
            forn = Fornitore()
            forn.set(line[0], line[1])
            self.fornitori.append(forn)
        pass

    def getAppalti(self):

        self.appalti = list()
        query = 'select * from appalti'
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        c = 0
        for line in rows:
            app = Appalto()
            app.set(line[0], line[1])
            c += 1
            self.appalti.append(app)

        return 1

    def getCommesse(self):

        self.commesse = list()
        query = 'select * from commesse'
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        c = 0
        for line in rows:
            com = Commessa()
            com.set(line[0], line[1])
            c += 1
            self.commesse.append(com)

        return 1

    def getDdt(self):

        self.ddt = list()
        query = 'select * from ddt'
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        c = 0
        for line in rows:
            ddt = Ddt()
            _id = line[0]
            data = line[1]
            filename = line[2]
            slag = line[3]
            ddt.set(_id, data, filename, slag)
            c += 1
            self.ddt.append(ddt)

        return 1

    def getCarichi(self):

        self.carichi = list()
        query = 'select * from carichi'
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        c = 0

        for line in rows:

            car = Carico()
            forn = Fornitore()
            mat = Materiale()
            com = Commessa()
            ddt = Ddt()
            forn.id = line[1]
            mat.id = line[3]
            ddt.id = line[5]
            com.id = line[6]
            caricato = line[7]

            for elemento in self.fornitori:
                if line[1] == elemento.id:
                    forn.set(elemento.id, elemento.desc)

            for elemento in self.materiali:
                if mat.id == elemento.id:
                    mat.set(elemento.id, elemento.desc, elemento.um,
                            elemento.qt, elemento.val)

            for elemento in self.ddt:
                if ddt.id == elemento.id:
                    ddt.set(elemento.id, elemento.datetime,
                            elemento.filename, elemento.slag)

            for elemento in self.commesse:
                if com.id == elemento.id:
                    com.set(elemento.id, elemento.desc)
            car.set(
                line[0],
                ddt,
                forn,
                line[2],
                mat,
                com,
                line[4],
                line[7]
                )
            c += 1
            self.carichi.append(car)
    
    def getScarichi(self):
        self.scarichi = list()
        query = 'select * from scarichi'
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        for scarico in rows:
            scar = Scarico()
            scar_id = scarico[0]
            scar_datetime = scarico[1]
            scar_id_mat = scarico[2]
            scar_id_subapp = scarico[3]
            scarico_id_commessa = scarico[4]
            scarico_qt = scarico[5]
            scarico_scaricato = scarico[6]
            mat = Materiale()
            app = Appalto()
            com = Commessa()

            for materiale in self.materiali:
                if materiale.id == scar_id_mat:
                    mat.set(materiale.id,materiale.desc,materiale.um,materiale.val,materiale.qt)
                
            for appalto in self.appalti:
                if appalto.id == scar_id_subapp:
                    app.set(appalto.id,appalto.desc)

            for commessa in self.commesse:
                if commessa.id == scarico_id_commessa:
                    com.set(commessa.id,commessa.desc)

            scar.set(scar_id,scar_datetime,mat,app,com,scarico_qt,scarico_scaricato)
            self.scarichi.append(scar)

    def getGiacenza(self):
        giac = list()
        for materiale in self.materiali:
            qt = 0
            for scarico in self.scarichi:
                if scarico.materiale.id == materiale.id and scarico.scaricato==0:
                    #print "[gesmag] : trovato scarico :: " + materiale.desc +' ::: '+str(materiale.qt)+' - '+str(scarico.qt) +' = '+str(materiale.qt-scarico.qt) 
                    qt += scarico.qt 
                    materiale.qt = materiale.qt - scarico.qt
            if qt > 0:
                print "[gesmag] : [getGiacenza] :" + materiale.desc + ":: scaricati :: " + str(qt) + ":: [GIACENZA] :: " + str(materiale.qt)
            for carico in self.carichi:
                if carico.materiale.id == materiale.id and carico.scaricato==0:
                    #print "[gesmag] : trovato carico ::" + materiale.desc +' ::: '+str(materiale.qt)+' - '+str(carico.qt)
                    materiale.qt = materiale.qt + carico.qt
            giac.append(materiale)
        return giac


    def setGiacenza(self):
        q="INSERT INTO `gesmag`.`giacenza` (`id_materiale`, `desc_materiale`, `um_materiale`, `giacenza_materiale`, `val_materiale`) VALUES (%i, %r, %r, %f, %f);"
        queryList = list()
        g = self.getGiacenza()
        for materiale in g:
            queryList.append(q % (int(materiale.id), str(materiale.desc), str(materiale.um), float(materiale.qt), float(materiale.val)))
        self.cursor.execute("TRUNCATE giacenza")
        "[gesmag] : Svuoto la tabella giacenza... ::"
        self.cnx.commit()
        for query in queryList:
            self.cursor.execute(query)
            self.cnx.commit()
            print query  + ' [OK] '
        return 1
