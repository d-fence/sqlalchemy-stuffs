#!/usr/bin/python
#- encoding=utf-8 -#
import os
import sqlalchemy as sa
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker,relationship, backref
from sqlalchemy.sql import functions

DATABASE = "comptoir.sqlite"

# on efface la db si elle existe
if os.path.exists(DATABASE):
    os.unlink(DATABASE)

engine = sa.create_engine('sqlite:///{}'.format(DATABASE), echo=False)

Base = declarative_base()

# Mapping des classes vers des tables


class Client(Base):
    __tablename__ = 't_clients'
    id = sa.Column(sa.Integer,primary_key=True)
    nom_cli = sa.Column(sa.String)
    commandes = relationship("Commande", backref="client")

class Produit(Base):
    __tablename__ = 't_produits'
    id = sa.Column(sa.Integer,primary_key=True)
    nom_prod = sa.Column(sa.String)
    pu = sa.Column(sa.Integer)
    detcoms = relationship("DetCom", backref="produit")
    
class Commande(Base):
    __tablename__ = 't_commandes'
    id = sa.Column(sa.Integer,primary_key=True)
    fkCli = sa.Column(sa.Integer, sa.ForeignKey('t_clients.id'))
    details = relationship("DetCom", backref="commande")
    
class DetCom(Base):
    __tablename__ = 't_detcom'
    fkCom = sa.Column(sa.Integer,sa.ForeignKey('t_commandes.id'), primary_key=True)
    fkProd = sa.Column(sa.Integer,sa.ForeignKey('t_produits.id'),primary_key=True)
    qte = sa.Column(sa.Integer)
    pu = sa.Column(sa.Integer)

Base.metadata.create_all(engine)

# Déclaration de la session
Session = sessionmaker(bind=engine)
# Instantiation de la session
session = Session()

# Ajout de données dans le DB

Quick = Client()
Quick.nom_cli = "Quick"

Carrefour = Client()
Carrefour.nom_cli = "Carrefour"

Ikea = Client()
Ikea.nom_cli = "Ikea"

Macdo = Client()
Macdo.nom_cli = "Macdo"

session.add_all([Quick,Carrefour,Ikea,Macdo])
session.commit()

Fromage = Produit()
Fromage.nom_prod = "10 tranches de frometon"
Fromage.pu = 1

Lait = Produit()
Lait.nom_prod = "1 L de lait"
Lait.pu = 2

Clous = Produit()
Clous.nom_prod = "1 Kg de clous"
Clous.pu = 3

Vis = Produit()
Vis.nom_prod = "1Kg de vis"
Vis.pu = 5

Viande = Produit()
Viande.nom_prod = "1Kg de viande de boeuf (ou cheval)"
Viande.pu = 15

Boulettes = Produit()
Boulettes.nom_prod = "Boulettes (au caca)"
Boulettes.pu = 2

Ketchup = Produit()
Ketchup.nom_prod = "1L de Ketchup"
Ketchup.pu = 2

Cornichons = Produit()
Cornichons.nom_prod = "1 Kg de cornichons"
Cornichons.pu = 2

session.add_all([Fromage,Lait,Clous,Vis,Viande,Boulettes,Cornichons])
session.commit()

# Une commande pour le Quick
qcom1 = Commande()
qcom1.client = Quick
qdc1 = DetCom()
qdc1.commande = qcom1
qdc1.produit = Fromage
qdc1.pu = 1
qdc1.qte = 1500
qdc2 = DetCom()
qdc2.commande = qcom1
qdc2.produit = Viande
qdc2.pu = 15
qdc2.qte = 100

# Une deuxième commande pour le Quick
qcom2 = Commande()
qcom2.client = Quick
qdc3 = DetCom()
qdc3.commande = qcom2
qdc3.produit = Ketchup
qdc3.pu = 2
qdc3.qte = 100
qdc4 = DetCom()
qdc4.commande = qcom2
qdc4.produit = Cornichons
qdc4.pu = 2
qdc4.qte = 25

# Une commande de clous et de vis pour Ikea
icom1 = Commande()
icom1.client = Ikea
idc1 = DetCom()
idc1.commande = icom1
idc1.produit = Clous
idc1.pu = 2
idc1.qte = 1500
idc2 = DetCom()
idc2.commande = icom1
idc2.produit = Vis
idc2.pu = 4
idc2.qte = 1250

session.add_all([qcom1,qcom2,icom1,qdc1,qdc2,qdc3,qdc4,idc1,idc2])
session.commit()

print """
### Bon, maintenant on fait des queries
### Une simple pour commencer: Tous les clients
"""
cquery = session.query(Client).order_by(Client.nom_cli)
print "La requête: {}".format(str(cquery))
print "Le resultat:"
for c in cquery:
    print c.nom_cli

print """
### Une requête un peu plus élaborée
### Les produits qui contiennent vi et classés par ordre de prix
"""
pquery = session.query(Produit).filter(Produit.nom_prod.like('%vi%')).order_by(Produit.pu)
print "La requête: {}".format(str(pquery))
print "Le resultat:"
for p in pquery:
    print "Produit: {} -- Prix: {}".format(p.nom_prod,p.pu)


print """
### Une reqête avec un having
### Les clients ayant plus de 1 commande
"""
hquery = session.query(Client.nom_cli).join(Commande).group_by(Client.nom_cli).having(functions.count(Commande.id) > 1)
print "La requête: {}".format(str(hquery))
print "Le resultat:"
for c in hquery:
    print c.nom_cli

print """
### Et maintenant, la difficile question du MAX
###
"""
squery = session.query(functions.sum(DetCom.pu * DetCom.qte).label('somme')).join(Commande).group_by(Commande).subquery()
mquery = session.query(functions.max(squery.c.somme).label('maximum')).as_scalar()
finalQuery = session.query(Client.nom_cli,functions.sum(DetCom.pu * DetCom.qte).label('fsomme')).join(Commande).join(DetCom).group_by(Commande).having(functions.sum(DetCom.pu*DetCom.qte)==mquery)
print "La requête: {}".format(str(finalQuery))
print "Le resultat:"
for m in finalQuery:
    print m

