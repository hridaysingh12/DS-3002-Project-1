# -*- coding: utf-8 -*-
"""
Created on Mon Mar 14 20:23:44 2022

@author: hrida
"""
import sqlite3
import statistics as stats
import numpy as np
import pandas as pd
import requests
import re
import json
import time
import csv



def askQuestion(question_string, accepted_input_list,multiple = False):
    accepted_input_list = [i.upper() for i in accepted_input_list]
    input_string = ", ".join(accepted_input_list[:-1]) +", or "+ accepted_input_list[-1]
    response = input(question_string + " Choose "+ input_string+" \n").strip().upper() 
    
    if multiple == False:
        while response not in accepted_input_list:
            response = input("You did not choose " + input_string+"! " +question_string + " Choose "+ input_string+" \n").strip().upper() 
        return response
        
    
    else:
        response_list = response.split(",")
        response_list = [i.strip() for i in response_list if i != ""]
        response_list_final = [i for i in response_list if i in accepted_input_list]
       
        return response_list_final
   


rawCSV = "https://raw.githubusercontent.com/JeffSackmann/tennis_atp/master/atp_matches_2019.csv"

orig_data = pd.read_csv(rawCSV)


file_type = askQuestion("What file format would you like to convert to?",["csv","json","db"])

if file_type == "CSV":
    file_name = "ETL_table.csv"
    orig_data.to_csv(file_name)

elif file_type == "JSON":
    file_name = "ETL_table.json"
    orig_data.to_json(file_name)

elif file_type == "DB":
    conn = sqlite3.connect('match_stats_database')
    c = conn.cursor()
    create_table = """CREATE TABLE IF NOT EXISTS matchstats( 
    tourney_id INT PRIMARY KEY,
    tourney_name TEXT,
    surface TEXT,
    draw_size TEXT,
    tourney_level TEXT,
    tourney_date TEXT,
    match_num TEXT,
    winner_id TEXT,
    winner_seed TEXT,
    winner_entry TEXT,
    winner_name TEXT,
    winner_hand TEXT,
    winner_ht TEXT,
    winner_ioc TEXT,
    winner_age TEXT,
    loser_id TEXT,
    loser_seed TEXT,
    loser_entry TEXT,
    loser_name TEXT,
    loser_hand TEXT,
    loser_ht TEXT,
    loser_ioc TEXT,
    loser_age TEXT,
    score TEXT,
    best_of TEXT,
    round TEXT,
    minutes INT,
    w_ace INT,
    w_df INT,
    w_svpt INT,
    w_1stIn INT,
    w_1stWon INT,
    w_2ndWon INT,
    w_SvGms INT,
    w_bpSaved INT,
    w_bpFaced INT,
    l_ace INT,
    l_df INT,
    l_svpt INT,
    l_1stIn INT,
    l_1stWon INT,
    l_2ndWon INT,
    l_SvGms INT,
    l_bpSaved INT,
    l_bpFaced INT,
    winner_rank INT,
    winner_rank_points INT,
    loser_rank INT,
    loser_rank_points INT);
    """
    c.execute(create_table)
    conn.commit()
    orig_data.to_sql('match_stats', conn, if_exists='replace', index = False)
    # conn.close()
    
    # c.execute('''  
    # SELECT * FROM match_stats
    #           ''')

    # for row in c.fetchall():
    #     print (row)
    
   
    
   
orig_data.columns = orig_data.columns.str.upper()
data = orig_data.copy()
data_new = pd.DataFrame()
data.columns = data.columns.str.upper()
orig_count = len(data.columns)
    
    

operation = askQuestion("Which operation would you like to perfrom on the data?", 
                        ["add columns", "drop columns","none"])

while operation != "NONE":
    

    if operation == "drop columns".upper():
        how_many_columns = askQuestion("How many columns do you want to drop?", ["one", "multiple"])
        if how_many_columns == "ONE":
            columns_to_change = askQuestion("Which column do you want to drop?", data_new.columns)
            data_new=data_new.drop(columns_to_change, axis = 1)
           
                
        elif how_many_columns == "MULTIPLE":
            columns_to_change = askQuestion("Which columns do you want to drop (seperate by commas)?", data_new.columns, multiple= True)
            data_new=data_new.drop(columns_to_change, axis = 1)
            
           
    
    elif operation == "add columns".upper():
        
        how_many_columns = askQuestion("How many columns do you want to add?", ["one", "multiple"])
        if how_many_columns == "ONE":
            columns_to_change = askQuestion("Which column do you want to drop?", list(set(orig_data.columns) - set(data_new)))
            data_new=pd.concat([data_new, orig_data[[columns_to_change]]], axis = 1)
    
    
        elif how_many_columns == "MULTIPLE":
            columns_to_change = askQuestion("Which columns do you want to drop (seperate by commas)?", list(set(orig_data.columns) - set(data_new)), multiple= True)
            # cols = [i.strip() for i in columns_to_change.split(",") ]
            
            data_new=pd.concat([data_new, orig_data[columns_to_change]], axis = 1)
            
    print("Number of Columns: " + str(len(data_new.columns)))
    print("Number of Rows: " + str(len(data_new)))
    print(data_new.head())
    
    operation = askQuestion("Which operation would you like to perfrom on the data?", 
                        ["add columns", "drop columns","none"])

print("Number of Columns: " + str(len(data_new.columns)))
print("Number of Rows: " + str(len(data_new)))
print(data_new.head())

# file_name = "modified_ETL_table.csv"
# data_new.to_csv(file_name)
    

create_table_list = """
    tourney_id INT,
    tourney_name TEXT,
    surface TEXT,
    draw_size TEXT,
    tourney_level TEXT,
    tourney_date TEXT,
    match_num TEXT,
    winner_id TEXT,
    winner_seed TEXT,
    winner_entry TEXT,
    winner_name TEXT,
    winner_hand TEXT,
    winner_ht TEXT,
    winner_ioc TEXT,
    winner_age TEXT,
    loser_id TEXT,
    loser_seed TEXT,
    loser_entry TEXT,
    loser_name TEXT,
    loser_hand TEXT,
    loser_ht TEXT,
    loser_ioc TEXT,
    loser_age TEXT,
    score TEXT,
    best_of TEXT,
    round TEXT,
    minutes INT,
    w_ace INT,
    w_df INT,
    w_svpt INT,
    w_1stIn INT,
    w_1stWon INT,
    w_2ndWon INT,
    w_SvGms INT,
    w_bpSaved INT,
    w_bpFaced INT,
    l_ace INT,
    l_df INT,
    l_svpt INT,
    l_1stIn INT,
    l_1stWon INT,
    l_2ndWon INT,
    l_SvGms INT,
    l_bpSaved INT,
    l_bpFaced INT,
    winner_rank INT,
    winner_rank_points INT,
    loser_rank INT,
    loser_rank_points INT
    """.split(",")
    
create_table_df = pd.Series(create_table_list).str.split(r"\n", expand = True).iloc[:,1].str.split(" ",expand = True).iloc[:,[4,5]]
create_table_df.columns = ["column", "data_type"]
create_table_df["column"] = create_table_df["column"].str.upper().str.strip()
create_table_df["data_type"] = create_table_df["data_type"].str.strip()

create_table_string = "CREATE TABLE IF NOT EXISTS matchstats2("
for i in data_new.columns:
    i_type = create_table_df.loc[create_table_df.column ==i,"data_type"].to_string(index=False)
    create_table_string += (" "+ i+" "+i_type+",")

create_table_string = create_table_string.strip(",") + ");"
    
file_type = askQuestion("What file format would you like to convert to?",["csv","json","db"])

if file_type == "CSV":
    file_name = "ETL_table_edited.csv"
    orig_data.to_csv(file_name)

elif file_type == "JSON":
    file_name = "ETL_table_edited.json"
    orig_data.to_json(file_name)

elif file_type == "DB":
    try:
        conn2 = sqlite3.connect('match_stats2_database_edited')
        c2 = conn2.cursor()
        c2.execute(create_table_string)
        conn2.commit()
        data_new.to_sql('match_stats2_edited', conn2, if_exists='replace', index = False)
        
        # c2.execute('''  
        # SELECT * FROM match_stats2_edited
        #           ''')
    
        
    
        # for row in c2.fetchall():
        #       print (row)
        
        conn.close()
        conn2.close()
    except:
        print("You tried to create and empty DB. Run program again and select columns this time.")
    
    
         
