import yaml

import io

# reading csv folder and match corrosponding yaml
from os import listdir
from os.path import isfile, join

path = "/root/Desktop/POC/Validation/csv"

yaml_path = "/root/Desktop/POC/Validation/yamls"

yfiles_list = []

files_list = []

print("###Validating YAML files for all data files\n")

for i in listdir(path):

    # print(path)

    # print(i)

    if isfile(join(path, i)):
        files_list.append(i.split('.')[0])

print(files_list)

for i in listdir(yaml_path):

    # print(path)

    # print(i)

    if isfile(join(yaml_path, i)):
        yfiles_list.append(i.split('.')[0])

print(yfiles_list)

if files_list.sort() == yfiles_list.sort():
    print("All yamls are present for all the files\n")

print("##Validating columns and data type for all the inbound files \n")

for i in files_list:

    '''

    yaml -id string, name string, age string

    data -id string, name string, age string



    '''

    # reading the yaml file

    with open(yaml_path + "/" + i + ".yaml", 'r') as stream:

        data_loaded = yaml.safe_load(stream)

    collist = []

    codict = {}

    for ij in data_loaded:
        dict1 = {data_loaded[ij][0]: str(data_loaded[ij][1])}

        codict.update(dict1)
     # reading the staging file

    import pandas as pd

    df = pd.read_csv(path + "/" + i + '.csv', header='infer', dtype={'id': int, 'name': str, 'sum': float})

    df_dict = {}

    for il in df.columns:
        df_dict.update({il: 'string'})

        # matching simultanious key and value in dataframe and yaml
    print('Yaml:: ',codict)
    print('CSV::',df_dict)
    if len(df_dict.keys()) !=  len(codict.keys()):
        load_check = 0
    else:
        for ik in df_dict.keys():

            if (df_dict[ik].lower() not in codict[ik].lower()) or (codict[ik].lower() not in df_dict[ik].lower()):

                print('col ' + ik + ' has different type/count ')

                load_check = 0

                break

            else:

                load_check = 1

        print("file " + path + "/" + i + '.csv' + " is correct")

if load_check == 1:
    print("##All columns and data type for all the inbound files are correct \n")
else:
    print("##Please check your CSV and YAML \n")