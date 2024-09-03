from collections import defaultdict
from datetime import datetime
import pandas as pd
import re
import sqlite3

# adds a check digit to UPC's to make them a full 12 digits
def add_check_digit(upc):
    if (type(upc) != str):
        return upc
    
    if len(upc) < 11:
        leadingZeros = '0' * (11 - len(upc))
        upc = leadingZeros + upc

    oddSum = 0
    evenSum = 0
    for idx in range(len(upc)):
        if (idx + 1) % 2 != 0:
            oddSum += int(upc[idx])
        else:
            evenSum += int(upc[idx])

    oddSum *= 3
    sum = oddSum + evenSum
    checkDigit = '0'
    if sum % 10 != 0:
        checkDigit = str(10 - (sum % 10))
    upc = upc + checkDigit
    return upc

# converts UPC E to UPC A
def upcE_to_upcA(upcE):
    if (type(upcE) != str):
        return upcE
    
    if (len(upcE) != 8):
        return upcE  # not a upcE
    
    firstFive = upcE[1:6]
    secondLast = upcE[6]

    if secondLast == '0':
        return '0' + firstFive[:2] + '00000' + firstFive[2:] + upcE[7]
    elif secondLast == '1':
        return '0' + firstFive[:2] + '10000' + firstFive[2:] + upcE[7]
    elif secondLast == '2':
        return '0' + firstFive[:2] + '20000' + firstFive[2:] + upcE[7]
    elif secondLast == '3':
        return '0' + firstFive[:3] + '00000' + firstFive[3:] + upcE[7]
    elif secondLast == '4':
        return '0' + firstFive[:4] + '00000' + firstFive[4:] + upcE[7]
    else:
        return '0' + firstFive + '0000' + secondLast + upcE[7]

# processes the inventory file from the POS system
def process_storetender_file(file, dbPath):
    dtypes = defaultdict(lambda: str)
    dtypes['CASE COST'] = float
    dtypes['MARGIN'] = float
    dtypes['MARKUP'] = float
    dtypes['PRICE'] = float
    headers = ['PLU NUMBER','PLU DESCRIPTION','LONG DESCRIPTION','SIZE','UOM',
               'BRAND NO','DEPT NO','SUB DEPT NO','FAMILY NO','SCALE USAGE',
               'VENDOR NO','ITEM NO','TAX 1 NO','TAX 2 NO','TAX 3 NO','CASE PLU',
               'LINK PLU','PACK SIZE','CASE COST','MARGIN','MARKUP','MULTIPLIER',
               'PRICE','FORMULA NO','WIC FOOD GROUP','MIN ON HAND','MAX ON HAND',
               'ON ORDER','ON HAND','OPENING BALANCE','FOOD STAMPS','WIC','WIC FV',
               'KIT','FOOD SERVICE ITEM','TAG NEEDED','CARRIES']
    data = pd.read_csv(file, on_bad_lines="skip", dtype=dtypes, names=headers)
    data.rename(columns={'PLU NUMBER': 'UPC'}, inplace=True)
    data = data.applymap(lambda x: str(x).strip())  # strip leading and trailing spaces
    data = data.replace(r'[^0-9a-zA-Z\s.]', '', regex=True)  # remove all non-alphanumeric ch (except space and .)

    return data