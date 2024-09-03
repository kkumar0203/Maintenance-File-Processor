import pandas as pd
import re
import sqlite3
import Utils as utils
from zpl import PRICE_ZPL, TPR_ZPL

print_debug = False

def parse_sil_txt(txt_file, header, sql_table_filter, dbPath):
    is_item_info = False
    temp_csv = "../files/temp.csv"
    w = open(temp_csv, 'w')
    w.write(header + '\n')
    with open(txt_file, 'r') as f:
        for line in f:
            # necessary item info is after the INSERT INTO... statement in txt file
            if 'INSERT INTO ' + sql_table_filter + ' VALUES' in line:
                is_item_info = True
                continue

            # sql insert statement has ended so after ';' there is no item info
            if ';' in line:
                is_item_info = False
            
            # if item info, parse the line into a csv
            if is_item_info:
                pattern = r'\((.*?)\)'  # reg ex to get text inside parentheses
                item_info = re.search(pattern, line)
                pattern = r"'(.*?)'"  # reg ex to get text inside single quotes
                # remove ',' from single quotes to preserve correct structure of csv file
                item_info = re.sub(pattern, lambda x: x.group(0).replace(',', ''),
                                    item_info.group(1))
                if print_debug:
                    print(item_info)
                w.write(item_info + '\n')  # write line to csv file
    w.close()

    data = pd.read_csv(temp_csv, dtype=str)  # make the returnable df from the csv

    return data

# formats and processes the temporary price reductions (TPR)
def process_tpr(tpr):
    tpr = to_correct_format(tpr)  # format df
    tpr = tpr[['UPC', 'PRICE','TPR PRICE', 'TPR START DATE', 'TPR END DATE']]  # filter cols

    # convert yyyyddd to mm/dd/yyyy
    tpr['TPR START DATE'] = pd.to_datetime(tpr['TPR START DATE'], format='%Y%j')
    tpr['TPR END DATE'] = pd.to_datetime(tpr['TPR END DATE'], format='%Y%j')
    tpr['TPR START DATE'] = tpr['TPR START DATE'].dt.strftime('%m/%d/%Y')
    tpr['TPR END DATE'] = tpr['TPR END DATE'].dt.strftime('%m/%d/%Y')

    # remove rows with 'nan' values for price or tpr price
    tpr = tpr[tpr['PRICE'].str.lower() != 'nan']
    tpr = tpr[tpr['TPR PRICE'].str.lower() != 'nan']

    return tpr

# formats and processes new items not yet in the store db
def process_new(new, is_drop_duplicates = True):
    # replace all non-alphanumeric ch (except space and .) from DESCRIPTION with a space
    new['DESCRIPTION'] = new['DESCRIPTION'].replace(r'[^0-9a-zA-Z\s.]', ' ', regex=True)

    new = to_correct_format(new)  # format df

    if is_drop_duplicates:  # remove duplicates (keep first occurance of item no)
        new = new.drop_duplicates(subset='VENDOR ITEM NUMBER', keep='first')  # remove duplicates based on ITEM NO, keep first row (primary UPC)


    # set EBT and Tax flags based on the department numbers
    # 1: GROCERY (EBT, NO TAX)
    # 3: GROCERY (NO EBT, TAX)
    # 4: NA BEVERAGES (EBT, TAX)
    # 10: HOT DELI (NO EBT, TAX)
    new.loc[new['GROUP'] == '317', 'POS DEPARTMENT'] = '3'  # puts feminine products into taxable
    new['FOOD STAMP'] = 'False'
    new['TAX FLAG 1'] = '0'
    new.loc[new['POS DEPARTMENT'] == '1', 'FOOD STAMP'] = 'True'
    new.loc[new['POS DEPARTMENT'] == '1', 'TAX FLAG 1'] = '0'
    new.loc[new['POS DEPARTMENT'] == '4', 'FOOD STAMP'] = 'True'
    new.loc[new['POS DEPARTMENT'] == '4', 'TAX FLAG 1'] = '1'
    new.loc[new['POS DEPARTMENT'] == '3', 'FOOD STAMP'] = 'False'
    new.loc[new['POS DEPARTMENT'] == '3', 'TAX FLAG 1'] = '1'
    new.loc[new['POS DEPARTMENT'] == '10', 'FOOD STAMP'] = 'False'
    new.loc[new['POS DEPARTMENT'] == '10', 'TAX FLAG 1'] = '1'
    
    new['VENDOR ITEM NUMBER'] = new['VENDOR ITEM NUMBER'].apply(lambda x: x[:-2])  # remove last two ch of ITEM NO

    new['POS DESCRIPTION'] = new['DESCRIPTION'].str[:19]  # make SHORT DESCRIPTION the first 19 ch of LONG DESCRIPTION

    new.loc[new['UNIT OF MEASURE CODE'] == '48', 'UNIT OF MEASURE CODE'] = 'OZ'
    new.loc[new['UNIT OF MEASURE CODE'] == '32', 'UNIT OF MEASURE CODE'] = 'SQFT'
    new.loc[new['UNIT OF MEASURE CODE'] == '1', 'UNIT OF MEASURE CODE'] = 'EA'
    new.loc[new['UNIT OF MEASURE CODE'] == '86', 'UNIT OF MEASURE CODE'] = 'G'
    new.loc[new['UNIT OF MEASURE CODE'] == '41', 'UNIT OF MEASURE CODE'] = 'FLOZ'
    new.loc[new['UNIT OF MEASURE CODE'] == '22', 'UNIT OF MEASURE CODE'] = 'LINFT'

    # include item size and uom in LONG DESCRIPTION
    new['DESCRIPTION'] = new['DESCRIPTION'] + ' ' + new['SIZE'].astype(float).astype(str) + new['UNIT OF MEASURE CODE']

    # filter columns
    req_cols = ['UPC', 'DESCRIPTION', 'POS DESCRIPTION', 'POS DEPARTMENT',
                 'FOOD STAMP', 'TAX FLAG 1', 'VENDOR ITEM NUMBER', 'CASE PACK', 'CASE COST', 'PRICE']
    
    new = new[req_cols]

    # remove rows with case cost or price 'nan' values
    new = new[new['CASE COST'].str.lower() != 'nan']
    new = new[new['PRICE'].str.lower() != 'nan']

    return new

# formats and processes price change items already in the store db
def process_pcu(pcu):

    pcu = to_correct_format(pcu)  # format df

    req_cols = ['UPC', 'VENDOR NO', 'PRICE', 'PRICE MULTIPLE', 'CASE COST']
    req_cols = ['UPC', 'PRICE', 'CASE COST']  # filter cols
    pcu = pcu[req_cols]

    # remove rows with case cost or price 'nan' values
    pcu = pcu[pcu['CASE COST'].str.lower() != 'nan']
    pcu = pcu[pcu['PRICE'].str.lower() != 'nan']

    return pcu

# formats and processes sale/ad items already in the store db
def process_ad(ad):
    ad = to_correct_format(ad)  # format df
    ad = ad[['UPC', 'PRICE', 'SALE PRICE MULTIPLE', 'SALE PRICE', 'SALE START DATE', 'SALE END DATE']]  # filter cols

    # convert yyyyddd to mm/dd/yyyy
    ad['SALE START DATE'] = pd.to_datetime(ad['SALE START DATE'], format='%Y%j')
    ad['SALE END DATE'] = pd.to_datetime(ad['SALE END DATE'], format='%Y%j')
    ad['SALE START DATE'] = ad['SALE START DATE'].dt.strftime('%m/%d/%Y')
    ad['SALE END DATE'] = ad['SALE END DATE'].dt.strftime('%m/%d/%Y')

    # remove rows with 'nan' values for price or tpr price
    ad = ad[ad['PRICE'].str.lower() != 'nan']
    ad = ad[ad['SALE PRICE'].str.lower() != 'nan']

    return ad

# merges tprs from the wholesales file with the store db
def get_tpr(tpr, st, dbPath, min_margin=0):
    st['ORG'] = st['UPC']
    st['UPC'] = st['UPC'].apply(utils.upcE_to_upcA)
    st = st[['UPC', 'ORG', 'ITEM NO', 'LONG DESCRIPTION', 'PACK SIZE', 'CASE COST']]
    tpr = process_tpr(tpr) 
    tpr = tpr.merge(st, on='UPC')  # only keep rows whose UPC exists in st
    tpr['UPC'] = tpr['ORG']
    tpr['SAVE'] = (pd.to_numeric(tpr['PRICE']) - pd.to_numeric(tpr['TPR PRICE'])).round(2)
    tpr['SAVE'] = tpr['SAVE'].apply(lambda x: f'{x:.2f}')
    tpr['UNIT COST'] = (pd.to_numeric(tpr['CASE COST']) / pd.to_numeric(tpr['PACK SIZE'])).round(2)
    tpr['MARGIN'] = ((1 - (pd.to_numeric(tpr['UNIT COST']) / pd.to_numeric(tpr['TPR PRICE']))) * 100).round(2)
    if (min_margin > 0):  # only keep TPR whose margins are > min_margin
        tpr = tpr[tpr['MARGIN'] > min_margin]
    to_sql_table(dbPath, tpr, "ALL TPR")
    return tpr

# merges sales/ads from the wholesales file with the store db
def get_ad(ad, st, dbPath):
    st['ORG'] = st['UPC']
    st['UPC'] = st['UPC'].apply(utils.upcE_to_upcA)
    st = st[['UPC', 'ORG', 'ITEM NO', 'LONG DESCRIPTION', 'CASE COST', 'PACK SIZE']]
    ad = process_ad(ad) 
    ad = ad.merge(st, on='UPC')  # only keep rows whose UPC exists in st
    ad['UPC'] = ad['ORG']

    ad['SAVE'] = (pd.to_numeric(ad['SALE PRICE MULTIPLE']) * pd.to_numeric(ad['PRICE']) - pd.to_numeric(ad['SALE PRICE'])).round(2)
    ad['SAVE'] = ad['SAVE'].apply(lambda x: f'{x:.2f}')

    ad['MARGIN'] = ((1 - ((pd.to_numeric(ad['CASE COST']) / pd.to_numeric(ad['PACK SIZE']) * pd.to_numeric(ad['SALE PRICE MULTIPLE'])) / pd.to_numeric(ad['SALE PRICE']))) * 100).round(2)
    ad['MARGIN'] = ad['MARGIN'].apply(lambda x: f'{x:.2f}')

    to_sql_table(dbPath, ad, "ALL SALE")
    return ad

# writes a ZPL file for the Zebra tag printer for the TPR items
def write_tpr_tags(tpr):
    with open('../files/uploadables/tpr_tags.zpl', 'w') as file:
        for idx, row in tpr.iterrows():
            formatted_string = TPR_ZPL.format(LONG_DESCRIPTION=row['LONG DESCRIPTION'],
                                              UPC=row['UPC'],
                                              ITEM_NO=row['ITEM NO'],
                                              TPR_PRICE=row['TPR PRICE'],
                                              TPR_START_DATE=row['TPR START DATE'],
                                              TPR_END_DATE=row['TPR END DATE'],
                                              SAVE=row['SAVE'],
                                              PRICE=row['PRICE'])
            file.write(formatted_string + '\n')

# writes a ZPL file for the Zebra tag printer for the new and price change items
def write_price_tags(data):
    with open('../files/uploadables/price_tags.zpl', 'w') as file:
        for idx, row in data.iterrows():
            formatted_string = PRICE_ZPL.format(LONG_DESCRIPTION=row['DESCRIPTION'],
                                              UPC=row['UPC'],
                                              ITEM_NO=row['VENDOR ITEM NUMBER'],
                                              PRICE=row['PRICE'])
            file.write(formatted_string + '\n')
    
# merges new items from the wholesales file with the store db
def get_new(new, st, dbPath, new_table_name, pcu_table_name, do_price_filter=True):
    st['ORG'] = st['UPC']
    st['UPC'] = st['UPC'].apply(utils.upcE_to_upcA)
    new = process_new(new)

    new_ = new[~new['UPC'].isin(st['UPC'])]  # actually new items that don't exist in st

    st = st.rename(columns={'PRICE': 'OLD PRICE', 'CASE COST': 'OLD CASE COST'})
    st = st[['UPC', 'ORG', 'DEPT NO', 'FOOD STAMPS', 'TAX 1 NO', 'OLD PRICE', 'OLD CASE COST']]
    pcu_with_new_format = new.merge(st, on='UPC')
    pcu_with_new_format['UPC'] = pcu_with_new_format['ORG']

    req_cols = ['UPC', 'DESCRIPTION', 'POS DESCRIPTION', 'DEPT NO',
                 'FOOD STAMPS', 'TAX 1 NO', 'VENDOR ITEM NUMBER', 'CASE PACK', 'CASE COST', 'PRICE', 'OLD PRICE', 'OLD CASE COST']
    pcu_with_new_format = pcu_with_new_format[req_cols]  # filter cols

    pcu_with_new_format['PRICE DIF'] = pd.to_numeric(pcu_with_new_format['PRICE']) - pd.to_numeric(pcu_with_new_format['OLD PRICE'])
    pcu_with_new_format['CASE COST DIF'] = pd.to_numeric(pcu_with_new_format['CASE COST']) - pd.to_numeric(pcu_with_new_format['OLD CASE COST'])

    if do_price_filter:
        mask = (pcu_with_new_format['PRICE DIF'] != 0) | (pcu_with_new_format['CASE COST DIF'] != 0)  # only keep rows where the price or the case cost has changed
        pcu_with_new_format = pcu_with_new_format[mask]

    pcu_with_new_format['UNIT COST'] = (pd.to_numeric(pcu_with_new_format['CASE COST']) / pd.to_numeric(pcu_with_new_format['CASE PACK'])).round(2)
    pcu_with_new_format['MARGIN'] = ((1 - (pd.to_numeric(pcu_with_new_format['UNIT COST']) / pd.to_numeric(pcu_with_new_format['PRICE']))) * 100).round(2)

    renames = {'DEPT NO': 'POS DEPARTMENT', 'FOOD STAMPS': 'FOOD STAMP', 'TAX 1 NO': 'TAX FLAG 1'}
    pcu_with_new_format = pcu_with_new_format.rename(columns=renames)

    return (new_, pcu_with_new_format)

# merges price change items from the wholesales file with the store db
def get_pcu(pcu, st, new, dbPath, table_name, do_price_filter=True, only_do_if_cost_change=False):
    new = process_new(new, is_drop_duplicates=False)
    st['ORG'] = st['UPC']
    st['UPC'] = st['UPC'].apply(utils.upcE_to_upcA)
    st = st.rename(columns={'PRICE': 'OLD PRICE', 'CASE COST': 'OLD CASE COST'})
    st = st[['UPC', 'ORG', 'PLU DESCRIPTION', 'LONG DESCRIPTION', 'DEPT NO', 'ITEM NO', 'FOOD STAMPS', 'TAX 1 NO', 'PACK SIZE', 'OLD PRICE', 'OLD CASE COST']]
    pcu = process_pcu(pcu)
    pcu = pcu.merge(st, on='UPC')  # only keep rows whose UPC is in st
    pcu = pcu[~pcu['UPC'].isin(new['UPC'])]  # only keep rows whose UPC is not already in 'new' format
    pcu['UPC'] = pcu['ORG']

    req_cols = ['UPC', 'LONG DESCRIPTION', 'PLU DESCRIPTION', 'DEPT NO',
                 'FOOD STAMPS', 'TAX 1 NO', 'ITEM NO', 'PACK SIZE', 'CASE COST', 'PRICE', 'OLD PRICE', 'OLD CASE COST']
    pcu = pcu[req_cols]  # filter cols

    pcu['PRICE DIF'] = pd.to_numeric(pcu['PRICE']) - pd.to_numeric(pcu['OLD PRICE'])
    pcu['CASE COST DIF'] = pd.to_numeric(pcu['CASE COST']) - pd.to_numeric(pcu['OLD CASE COST'])

    if do_price_filter:
        mask = (pcu['PRICE DIF'] != 0) | (pcu['CASE COST DIF'] != 0)
        pcu = pcu[mask]

    if only_do_if_cost_change:
        mask = pcu['CASE COST DIF'] != 0
        pcu = pcu[mask]

    pcu['UNIT COST'] = (pd.to_numeric(pcu['CASE COST']) / pd.to_numeric(pcu['PACK SIZE'])).round(2)
    pcu['MARGIN'] = ((1 - (pd.to_numeric(pcu['UNIT COST']) / pd.to_numeric(pcu['PRICE']))) * 100).round(2)

    renames = {'LONG DESCRIPTION': 'DESCRIPTION', 'PLU DESCRIPTION': 'POS DESCRIPTION', 'DEPT NO': 'POS DEPARTMENT',
               'FOOD STAMPS': 'FOOD STAMP', 'TAX 1 NO': 'TAX FLAG 1', 'ITEM NO': 'VENDOR ITEM NUMBER',
                'PACK SIZE': 'CASE PACK'}
    pcu = pcu.rename(columns=renames)

    return pcu

def to_correct_format(data):
    data = data.copy()
    data = data.applymap(lambda x: str(x).strip())  # strip leading and trailing spaces
    data = data.replace(r'[^0-9a-zA-Z\s.]', '', regex=True)  # remove all non-alphanumeric ch (except space and .)
    data = data.applymap(lambda x: str(x).lstrip('0'))  # remove leading zeroes
    data['UPC'] = data['UPC'].apply(utils.add_check_digit)  # make UPC full 12 digits with zeroes and check digit
    return data

def to_sql_table(dbPath, data, tableName):
    conn = sqlite3.connect(dbPath)
    data.to_sql(tableName, con=conn, if_exists='replace', index=False)
    conn.commit()
    conn.close()
    if print_debug:
        print("done adding ",tableName, "to db")

def parse(stFile, silFile, uploadable_new_items, uploadable_pcu_items, uploadable_tprs, uploadable_ads, dbPath, only_do_if_cost_change, min_tpr_margin):
    st = utils.process_storetender_file(stFile, dbPath)
    
    new_header = 'UPC,POS DESCRIPTION,POS DEPARTMENT,GROUP,SUB GROUP,REPORT CODE,CASE PACK,UNIT OF MEASURE CODE,SIZE,VENDOR NUMBER,DESCRIPTION,PRICE MULTIPLE,PRICE,MIX MATCH CODE,PRICE METHOD,CASE COST,FOOD STAMP,TAX FLAG 1,SCALE FLAG,PRICE REQUIRED FLAG,VISUAL VERIFY FLAG,QUANTITY REQUIRED FLAG,QUANTITY PROHIBIT FLAG,WIC FLAG,VENDOR ITEM NUMBER,ITEMIZER 6,ITEMIZER 7'
    new_unprocessed = parse_sil_txt(silFile, new_header, 'URM_NEW', dbPath)
    new, pcu_with_new_format = get_new(new_unprocessed.copy(), st.copy(), dbPath, 'NEW', 'PCU (NEW FORMAT)')
    new_2_unprocessed = parse_sil_txt(silFile, new_header, 'URM_CHG', dbPath)
    new_2, pcu_with_new_format_2 = get_new(new_2_unprocessed.copy(), st.copy(), dbPath, 'NEW_CHG', 'PCU (NEW FORMAT)_CHG')
    new = pd.concat([new, new_2], ignore_index=True)
    new.insert(10, "ITEM VENDOR ID", "1")
    numerical_cols = ['UPC', 'POS DEPARTMENT', 'TAX FLAG 1', 'VENDOR ITEM NUMBER', 'CASE PACK', 'CASE COST', 'PRICE']
    new = new.replace('nan', pd.NA)
    new = new.dropna(subset=numerical_cols)

    new.to_csv(uploadable_new_items, index=False, header=False)
    to_sql_table(dbPath, new, "FINAL NEW")

    pcu_header = 'UPC,VENDOR NO,PRICE,PRICE MULTIPLE,CASE COST'
    pcu_unprocessed = parse_sil_txt(silFile, pcu_header, 'URM_PCU', dbPath)
    new_ = pd.concat([new_unprocessed, new_2_unprocessed], ignore_index=True)
    pcu = get_pcu(pcu_unprocessed.copy(), st.copy(), new_.copy(), dbPath, 'PCU',only_do_if_cost_change=only_do_if_cost_change)
    pcu_2_unprocessed = parse_sil_txt(silFile, pcu_header, 'URM_PCD', dbPath)
    pcu_2 = get_pcu(pcu_2_unprocessed.copy(), st.copy(), new_.copy(), dbPath, 'PCD', only_do_if_cost_change=only_do_if_cost_change)
    pcu = pd.concat([pcu_with_new_format, pcu_with_new_format_2, pcu, pcu_2], ignore_index=True)
    pcu.insert(10, "ITEM VENDOR ID", "1")
    numerical_cols = ['UPC', 'POS DEPARTMENT', 'TAX FLAG 1', 'CASE PACK', 'CASE COST', 'PRICE']
    pcu = pcu.replace('nan', pd.NA)
    pcu = pcu.dropna(subset=numerical_cols)

    pcu.to_csv(uploadable_pcu_items, index=False, header=False)
    to_sql_table(dbPath, pcu, "FINAL PCU")

    tpr_header = 'UPC,VENDOR NUMBER,PRICE,PRICE MULTIPLE,CASE COST,TPR PRICE MULTIPLE,TPR PRICE,TPR START DATE,TPR END DATE,PRICE METHOD'
    tpr = parse_sil_txt(silFile, tpr_header, 'URM_TPN', dbPath)
    tpr = get_tpr(tpr.copy(), st.copy(), dbPath, min_margin=min_tpr_margin)
    numerical_cols = ['UPC', 'PRICE', 'TPR PRICE']
    tpr = tpr.replace('nan', pd.NA)
    tpr = tpr.dropna(subset=numerical_cols)

    tpr.to_csv(uploadable_tprs, index=False, header=False)

    ad_header = 'UPC,VENDOR NUMBER,PRICE,PRICE MULTIPLE,CASE COST,SALE PRICE MULTIPLE,SALE PRICE,SALE START DATE,SALE END DATE'
    ad = parse_sil_txt(silFile, ad_header, 'URM_CPN', dbPath)
    ad = get_ad(ad.copy(), st.copy(), dbPath)
    numerical_cols = ['UPC', 'PRICE', 'SALE PRICE MULTIPLE','SALE PRICE']
    ad = ad.replace('nan', pd.NA)
    ad = ad.dropna(subset=numerical_cols)

    ad.to_csv(uploadable_ads, index=False, header=False)

    pcu_only_price_dif = pcu[pd.to_numeric(pcu['PRICE DIF']) != 0.0]

    write_price_tags(pd.concat([new, pcu_only_price_dif]))
    write_tpr_tags(tpr)
    