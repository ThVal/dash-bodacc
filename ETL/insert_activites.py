from sqlalchemy import create_engine
import pandas as pd
import numpy as np

communes = pd.read_csv("communes-departement-region.csv")
divisions_ape = pd.read_csv("divisions_ape_4.csv", sep = ";")
bodacc1 = pd.read_csv("data_2020_24.csv")
bodacc2 = pd.read_csv("data_2020_25_100.csv")
bodacc = pd.concat([bodacc1, bodacc2])


#Drop des duplicates code postal
communes_unique = communes.drop_duplicates(subset = "code_postal")
# communes_unique

#merge les csv data_test et communes sur le code postal
merge = bodacc.merge(communes_unique, on= "code_postal").drop_duplicates()
# merge

#On drop la colonne code_region qui nous est inutile
result = merge.drop(columns="code_region")
# result

# Récuperation des deux premiers chiffre du code ape pour les ajouter dans une nouvelle colonne "code secteur"
result['code_secteur'] = result["code_ape"].str[:2]
# result

toto = divisions_ape
toto

# Changement de la colonne code secteur en int64
result = result.astype({'code_secteur': 'int64'})
# result


final_dataframe = toto.merge(result, on= "code_secteur")
# final_dataframe

# data frame qui ressemble à la table activites
activite_table = final_dataframe[["code_ape", "activite_insee", "code_secteur", "Libelle"]]
activite_table = activite_table.rename(columns={"Libelle": "libelle"})
# print(activite_table)

#nettoyage et préparation de la table forme_juridiques

table_forme_juridique = final_dataframe[["forme_juridique"]]
table_forme_juridique
table_forme_juridique_final = table_forme_juridique["forme_juridique"].str.lower()
table_forme_juridique_final = table_forme_juridique_final.drop_duplicates()
table_forme_juridique_final = pd.DataFrame(data = table_forme_juridique_final)
table_forme_juridique_final.drop_duplicates()
test = table_forme_juridique_final["forme_juridique"].str.replace(r'[\(\)\d]+', '')
test.drop_duplicates()
forme_juridique = pd.DataFrame(data = test)
# forme_juridique


# mise en place de la données pour la table localisation:

localisations = communes_unique[["longitude","latitude", "code_postal", "nom_departement", "nom_region", "nom_commune_complet"]]
localisations = localisations.rename(columns = {"nom_departement" : "departement", "nom_region": "region", "nom_commune_complet" : "ville"})
localisations.dropna()

# insert_activites()

# Connection à la db
uri = f'postgres://{"steeven2"}:{"toto"}@{"127.0.0.1"}:{"5432"}/{"bodacc_test"}'
engine = create_engine(uri)
# Pour mettre toute les donnée du df dans la db
activite_table.to_sql(name="activites", con=engine, if_exists="append", index=False)
forme_juridique.to_sql(name= "forme_juridiques", con=engine, if_exists="append", index=False)
# flows.to_sql(name="flows", con=engine, if_exists="append", index=False)
localisations.to_sql(name= "localisations", con=engine, if_exists="append", index=False)
