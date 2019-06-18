import gspread
from oauth2client.service_account import ServiceAccountCredentials
import csv
import re
import numpy as np
import math
from spellchecker import SpellChecker
import sys
import copy
from gspread_formatting import *
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

# use creds to create a client to interact with the Google Drive API
scope = ['https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive.file',
        'https://www.googleapis.com/auth/drive']
creds = ServiceAccountCredentials.from_json_keyfile_name('client_secret.json', scope)
client = gspread.authorize(creds)
start_cell = 'E4'
start_col = 'E'
start_row = 4
answer_column = 4
max_codes_per_response = 7
format_cell_list = []
spell = SpellChecker()

def load_sheet(file_name, response_url, glossary_url, opposites_url):
    print('Coding ' + file_name + '...')
    answer_sheet = client.open_by_url(response_url).get_worksheet(get_sheet_index(response_url))
    read_responses(response_url,read_glossary(glossary_url),read_opposites(opposites_url))

def get_sheet_index(url):
    worksheet_list = client.open_by_url(url).worksheets()
    id = url.split('=')[1]
    count = 0
    for sheet in worksheet_list:
        if id == str(sheet.id):
            name = sheet.title
            break
        count = count + 1
    return count

def load_sheets(file_name):
    url_sheet = client.open(file_name).sheet1
    num_sheets = len(url_sheet.col_values(1))
    for i in range(2,num_sheets+1):
        print('Coding ' + url_sheet.cell(i,1).value + '...')
        read_responses(url_sheet.cell(i,2).value,read_glossary(url_sheet.cell(i,3).value))

def read_glossary(file_url):
    glossary = client.open_by_url(file_url).get_worksheet(get_sheet_index(file_url))
    glossary_dict = {}

    print('Importing Glossary...')
    #Copies google sheet glossary to csv file
    with open('glossary.txt', 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerows(glossary.get_all_values())

    #writes glossary to dictionary
    with open('glossary.txt') as glossary_file:
        reader = csv.reader(glossary_file, delimiter=',')
        #skips header row
        next(reader)
        for row in reader:
            for element in row:
                if element:
                    glossary_dict[element.lower().rstrip()] = str(row[0].rstrip())
    return glossary_dict

def read_opposites(file_url):
    opposites = client.open_by_url(file_url).get_worksheet(get_sheet_index(file_url))
    opposite_pairs = []

    print('Reading Opposites...')
    #Copies google sheet glossary to csv file
    with open('opposites.txt', 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerows(opposites.get_all_values())

    #writes glossary to dictionary
    with open('opposites.txt') as opposites_file:
        reader = csv.reader(opposites_file, delimiter=',')
        #skips header row
        next(reader)
        for row in reader:
            opposite_pairs.append((row[0].rstrip(),row[1].rstrip()))
    #print(opposite_pairs)
    return opposite_pairs

def generate_phrases(response):
    split = list(filter(None,re.split(',|\s|\.', response)))
    misspelled = spell.unknown(split)
    out = copy.copy(split)
    if misspelled:
        for word in split:
            if word in misspelled:
                word = spell.correction(word)

    for i in range(len(split)-2):
        out.append(split[i] + ' ' + split[i+1])
        out.append(split[i] + ' ' + split[i+1] + ' ' + split[i+2])
    if len(split) > 1:
        out.append(split[len(split)-2] + ' ' + split[len(split)-1])
    return out

def get_current_codes(length, answer_sheet):
    column_list = []
    for i in range(max_codes_per_response):
        column_list.append([None]*length)
        column = answer_sheet.col_values(answer_column+i+1)
        for j in range(len(column)):
            column_list[i][j] = column[j]
        del column_list[i][:3]
    return list(zip(*column_list))

def index_of_first_empty_cell_in_row(cell_list, row):
    code_list = []
    index = (row - start_row) * max_codes_per_response
    while cell_list[index].value:
        code_list.append(cell_list[index].value)
        index = index + 1
    return (index, code_list)

def read_responses(file_url, glossary, opposite_pairs):
    analyzer = SentimentIntensityAnalyzer()
    answer_sheet = client.open_by_url(file_url).get_worksheet(get_sheet_index(file_url))
    responses = answer_sheet.col_values(answer_column)
    #Remove top 3 blank lines
    del responses[:3]
    num_phones = len(responses)
    end_cell = chr(ord(start_col)+max_codes_per_response-1) + str(num_phones+4)
    cell_range = start_cell + ':' + end_cell
    cell_list = answer_sheet.range(cell_range)
    code_list = []
    counter = 1
    fmt = cellFormat(
    backgroundColor=color(1, 1, 0.7)
    )
    call = 0
    current_row = start_row
    #Search responses for key words and replace with codes from glossary
    for response in responses:
        used_codes = []
        #print('%r --- %s' % (response,analyzer.polarity_scores(response).get('compound')))
        score = analyzer.polarity_scores(response).get('compound')
        phrase_list = generate_phrases(response.lower())
        row_output = index_of_first_empty_cell_in_row(cell_list,current_row)
        index = row_output[0]
        used_codes = row_output[1]
        for phrase in phrase_list:
            if phrase in glossary:
                code = glossary[phrase]
                tuple = [pair for pair in opposite_pairs if code in pair]
                if tuple:
                    if score < 0:
                        code = tuple[0][1]
                        print(code)
                if code not in used_codes:
                    cell_list[index].value = code
                    cell = chr(ord(start_col) + index % max_codes_per_response) + str(current_row)
                    range = cell + ':' + cell
                    tup = (range, fmt)
                    format_cell_list.append(tup)
                    used_codes.append(code)
                    index = index + 1
        current_row = current_row + 1

        counter = counter + 1
        progress(counter, num_phones, status='Coding Responses')

    print('')
    print(format_cell_list[0])
    # Update in batch
    answer_sheet.update_cells(cell_list)
    format_cell_ranges(answer_sheet,format_cell_list)

def progress(count, total, status=''):
    bar_len = 60
    filled_len = int(round(bar_len * count / float(total)))

    percents = round(100.0 * count / float(total), 1)
    bar = '=' * filled_len + '-' * (bar_len - filled_len)

    sys.stdout.write('[%s] %s%s ...%s\r' % (bar, percents, '%', status))
    sys.stdout.flush()
