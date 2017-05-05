# -*- coding: utf-8 -*-
"""
Created on Fri Dec  2 17:33:21 2016

@author: User
"""
import os
import pdb

path = 'D:\data\SARAH\\20161110.DUMP.G14.SARAH'
path_file = os.path.join(path, '20161109.g14_metier.dump')
path_output = 'D:\data\SARAH\data_from_extract_python2'

import time

def brut_extract():
    count = 0
    indicateur = 0
#    en_cours = False
    save_name_table = open('temp.txt', 'w', encoding='utf8')
    with open(path_file, "r", encoding='utf8') as dump:
        for line in dump:   
            count += 1
            if 'signa' in line or 'SIGNA' in line:
                print(line, count)
            if count > 9000:
                if line.strip() != '':
#                    print(line[:-1])

#            if 'BIEN_ID' in line:
#            if line.startswith('COPY'):
#                if en_cours:
#                    save_name_table.close()
#                print(count)
#                print(line)
##                print(count)
#                name = line.split(' ')[1]
#                name = name.replace('"', 'g')
#                indicateur += 1
#                print(name)    
#                path_name = os.path.join(path_output, 'extract', name + '.txt')
#                if not os.path.exists(path_name) and 'datadocument' not in name: 
#                    en_cours = True
#                    try:
#                        #save_name_table = open(path_output, 'w', encoding='utf8')
#                        print('file ', name, ' was saved')
#                    except:
#                        pdb.set_trace()
#                else:
#                    en_cours = False
#                    if 'datadocument' in name:
#                        # doesn't work => useless                        
#                        for k in range(1244915, 1273949):
#                            continue
                        #pdb.set_trace()                                      
                        
#            if en_cours:
#                print(line)
                    save_name_table.write(line)
            if count > 25000:
                save_name_table.close()
                break


def _parse_first_line(line):
    assert line.startswith('COPY')
    hearders = line.split('(')[1].split(')')[0]
    return hearders.replace(', ', '\t')


def extract_to_csv():
    path_extract_brut = os.path.join(path_output, 'extract')
    path_csv = os.path.join(path_output, 'csv')
    for file in os.listdir(path_extract_brut):
#        if file in ['affectfacade.txt', 'alerte.txt']: if we want a select
            
        assert file[-4:] == '.txt'
        
        path_file_txt = os.path.join(path_extract_brut, file)
        path_file_csv = os.path.join(path_csv, file[:-4] + '.csv')
        
        with open(path_file_txt, "r", encoding='utf8') as source:
            with open(path_file_csv, "w", encoding='utf8') as csv:
                count_line = 0
                for line in source:
                    if line.startswith('\\.'):
                        break
                    if count_line == 0:
                        csv.write(_parse_first_line(line) + '\n')
                    else:
                        csv.write(line)
                    count_line += 1


path = 'D:\data\SARAH\\20161110.DUMP.G14.SARAH'
path_file = os.path.join(path, '20161109.g14_sig.dump')
path_file = os.path.join(path, '20161109.g14_jbpm.dump')
path_file = os.path.join(path, '20161109.g14_metier.dump')
path_output = 'D:\data\SARAH\sig_from_extract_python'



brut_extract()     
extract_to_csv()