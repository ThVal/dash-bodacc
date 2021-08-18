import xml.etree.ElementTree as et
import csv
import time
import glob, os
from parserr import parsing
from tqdm import tqdm

os.chdir("./.")
total_entrees = 0
y = 0
nb_entree_ds_fichier = 0
nb_fil_total = 0
""" chrono on """
start_time = time.time()


""" .xml file counter """
for file in glob.glob("*.xml"):
    nb_fil_total += 1
print(nb_fil_total)


fieldnames = ['type', 'siren', 'forme_juridique', 'activite', 'activite_insee', 'code_ape', 'code_postal', 'date_immat']

#context manager > on crée un CSV 
with open('data_2020.csv', 'w', newline='') as output_file:

            dict_writer = csv.DictWriter(output_file, fieldnames)
            dict_writer.writeheader()


            for file in glob.glob("*.xml"):

                print(file)
                y +=1

                nb_entree_ds_fichier = 0

                tree = et.parse(file)
                root = tree.getroot()

                date = root.findtext('dateParution')
                x = root.findall('.//avis')

                for root1 in tqdm(root.iter("avis"), total=len(x), desc='Progress'):
                    nb_entree_ds_fichier += 1
                    total_entrees += 1

                    row = parsing(root1, date)

                    #attention on ecrit un seul row donc writerow et non writerows !!!
                    dict_writer.writerow(row)     
    
#database.add_entreprise(liste)

""" time counter """
end_time = time.time()
temps = end_time - start_time
print(f"le temps d'execution du script est de {round(temps)} s, soit {round(temps / 60)} min , soit {round(temps/3600)} heures")








