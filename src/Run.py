
from Urm import parse

# runs the URM Maintenance file processor based based on the parameters below

'''
converts the maintenance file from URM into a csv file that can be uploaded into the POS system.
The uploadable csv files will have any new items delivered or any price changes. They will also
have any TPR (temporary price reductions) and sales. Also generated are ZPL files that can be sent
to the Zebra printer to print tags for all new/price change items.

'''


dbPath = "../db/urm.db"
silFile = '../files/SIL.TXT'
stFile = '../files/INV08272024.csv'
uploadable_new_items = "../files/uploadables/uploadable_new.csv"
uploadable_pcu_items = "../files/uploadables/uploadable_pcu.csv"
uploadable_tprs = "../files/uploadables/uploadable_tprs.csv"
uploadable_ads = "../files/uploadables/uploadable_ads.csv"

parse(stFile, silFile, uploadable_new_items, uploadable_pcu_items, uploadable_tprs, uploadable_ads, dbPath, only_do_if_cost_change=False, min_tpr_margin=40)