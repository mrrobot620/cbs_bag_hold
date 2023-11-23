import pandas as pd
from datetime import datetime
import pytz
import logging

time_zone = pytz.timezone('Asia/Kolkata')
current_time= datetime.now(time_zone)

# logging.basicConfig(format='%(asctime)s %(message)s' , datefmt='%m/%d/%Y %I:%M:%S %p' , filename='auto_pendency.logs' , encoding='utf-8' , level=logging.DEBUG )

logging.basicConfig(format='%(asctime)s %(message)s' , datefmt='%m/%d/%Y %I:%M:%S %p' , filename='auto_pendency.logs' , level=logging.DEBUG )


try:
    df_secondary = pd.read_csv('ykb_secondary_pending_abhi.csv')
except Exception as E:
    logging.warning(f"Error Secondary File not found:  {E}")
try:
    outbond_df = pd.read_csv('ykb_outbound.csv')
except Exception as E:
    logging.warning(f"Error Outbound file not Found: {E}")
try:
    outbond_12 = pd.read_csv("ykb_outbond_pending_greater_than_12.csv")
except Exception as E:
    logging.warning(f"Error Outbound12 File not found {E}")
try:
    df_ppph1 = pd.read_csv('ykb_pendency_automation_PPPH.csv')
except Exception as E:
    logging.warning(f"Error PPPH File not Found {E}")
try:
    df_bagging = pd.read_csv('ykb_bagging_pending_automation_abhi.csv')
except Exception as E:
    logging.warning(f"Error while reading the Bagging File {E}")
try:
    df = pd.read_csv("Pendency_automation_report_ageing_greater_than_12.csv")
except Exception as E:
    logging.warning(f"Error while reading PPPH12 File {E}")
try:
    outbond_xd = pd.read_csv('ykb_outbond_crossdock_abhi.csv')
except Exception as E:
    logging.warning(f"Error while Reading the Outbond XD file {E}")
try:
    outbond_sl = pd.read_csv('outbond_semi_large_abhi.csv')
except Exception as E:
    logging.warning(f"Error Outbond SL File not found {E}")
try:
    outbound_12_xd = pd.read_csv("ykb_outbound_xd_greater_than_12.csv")
except Exception as E:
    logging.warning(F"Error while reading Outbound XD 12 File: {E}")
try:
    outbound_12_sl = pd.read_csv("ykb_outbound_sl_greater_than-12.csv")
    # Spelling Mistake
except Exception as E:
    logging.warning(f"Error while reading Outbound SL 12 File  {E}")


try:
    df_secondary_zo = df_secondary[df_secondary['bag_type_ph'] == "ZO"]
    df_secondary_zo['bag_facility_source_name'].fillna("Not Found" , inplace=True)
    df_secondary_zo_ph  = df_secondary_zo[df_secondary_zo['bag_facility_source_name'].str.contains("DEL_PL|Bhiwadi|Bhiwani|Bilaspur")]
    df_secondary_zo_sph = df_secondary_zo[~df_secondary_zo.index.isin(df_secondary_zo_ph.index)]
    df_secondary_zo_ph1 = df_secondary_zo_ph.groupby('bag_facility_source_name').size()
    df_secondary_zo_sph1 = df_secondary_zo_sph.groupby('bag_facility_source_name').size()

    df_secondary_b5 = df_secondary[df_secondary['bag_type_ph'] == "B5"]
    df_secondary_b5['bag_facility_source_name'].fillna("Not Found" , inplace=True)
    df_secondary_b5_ph = df_secondary_b5[df_secondary_b5['bag_facility_source_name'].str.contains("DEL_PL|BHiwadi|Bhiwani|Bilaspur")]
    df_secondary_b5_sph = df_secondary_b5[~df_secondary_b5.index.isin(df_secondary_b5_ph.index)]
    df_secondary_b5_ph1 = df_secondary_b5_ph.groupby('bag_facility_source_name').size()
    df_secondary_b5_sph1 = df_secondary_b5_sph.groupby('bag_facility_source_name').size()
except Exception as E:
    logging.error(f"Error while Processing Secondary File:  {E}")


# OutBond Pending Automation Part
try:
    outbound_total = outbond_df['tracking_id_merchant'].sum()
    outbound_xd_total = outbond_xd['tracking_id_merchant'].sum()
    outbound_sl_total = outbond_sl['tracking_id_merchant'].sum()
except Exception as E:
    logging.error(f"Error while Processing OB Files:  {E}")


try:
    df_ppph= df_ppph1[df_ppph1['shipment_facility_current_name'] == "MotherHub_YKB"]
    ykb_ppph = df_ppph['tracking_id_ekart'].sum()
    other_mh  = df_ppph1[~df_ppph1.index.isin(df_ppph.index)]
    other_mh_count = other_mh['tracking_id_ekart'].sum()
    df_ppph_zo = df_ppph[df_ppph['bag_type_ph'] == "ZO"]
    df_ppph_zo_ph  = df_ppph_zo[df_ppph_zo['bag_facility_source_name'].str.contains("DEL_PL|Bilaspur|Bhiwani|Bhiwadi")]
    zo_ph_shipment_count  = df_ppph_zo_ph['tracking_id_ekart'].sum()
    df_ppph_zo_sph = df_ppph_zo[~df_ppph_zo.index.isin(df_ppph_zo_ph.index)]
    zo_sph_shipment_count = df_ppph_zo_sph['tracking_id_ekart'].sum()
    df_ppph_b5 = df_ppph[df_ppph['bag_type_ph'] == "B5"]
    df_ppph_b5_ph = df_ppph_b5[df_ppph_b5['bag_facility_source_name'].str.contains("DEL_PL|Bilaspur|Bhiwani")]
    b5_ph_shipment_count = df_ppph_b5_ph['tracking_id_ekart'].sum()
    df_ppph_b5_sph = df_ppph_b5[~df_ppph_b5.index.isin(df_ppph_b5_ph.index)]
    b5_sph_shipment_count = df_ppph_b5_sph['tracking_id_ekart'].sum()
except Exception as E:
    logging.debug(f": Error while Processing the PPPH Files : {E}")


# Bagging Pending 
try:
    df_bagging_zo = df_bagging[df_bagging['bag_type_ph'] == "ZO"]
    df_bagging_zo['bag_facility_source_name'].fillna("Not Found" , inplace=True)
    df_bagging_zo_ph = df_bagging_zo[df_bagging_zo['bag_facility_source_name'].str.contains("DEL_PL|Bilaspur|Bhiwadi|Bhiwani")]
    df_bagging_zo_sph = df_bagging_zo[~df_bagging_zo.index.isin(df_bagging_zo_ph.index)]
    zo_ph_bagging_count = df_bagging_zo_ph.groupby("bag_facility_source_name").size()
    zo_sph_bagging_count = df_bagging_zo_sph.groupby('bag_facility_source_name').size()
    df_bagging_b5 = df_bagging[df_bagging['bag_type_ph']== "B5"]
    df_bagging_b5['bag_facility_source_name'].fillna("Not Found" , inplace=True)
    df_bagging_b5_ph = df_bagging_b5[df_bagging_b5['bag_facility_source_name'].str.contains("DEL_PL|Bilaspur|Bhiwadi|Bhiwani")]
    df_bagging_b5_sph = df_bagging_b5[~df_bagging_b5.index.isin(df_bagging_b5_ph.index)]
    b5_ph_bagging_count = df_bagging_b5_ph.groupby("bag_facility_source_name").size()
    b5_sph_bagging_count = df_bagging_b5_sph.groupby("bag_facility_source_name").size()
except Exception as E:
    logging.warning(f": Error while Processing the Bagging File : {E}")

try: 
    df_zo = df[df['bag_type_ph'] == "ZO"]
    df_zo
    df_zo_ph = df_zo[df_zo["bag_facility_source_name"].str.contains("DEL_PL|Bilaspur|Bhiwani")]
    df_zo_ph_wise = df_zo_ph.groupby('bag_facility_source_name').size()
    df_zo_sph = df_zo[~df_zo.index.isin(df_zo_ph.index)]
    df_zo_sph_wise = df_zo_sph.groupby('bag_facility_source_name').size()
    df_b5 = df[df['bag_type_ph'] == "B5"]
    df_b5
    df_b5_ph = df_b5[df_b5['bag_facility_source_name'].str.contains("DEL_PL|Bilaspur|Bhiwani")]
    df_b5_ph_wise  = df_b5_ph.groupby('bag_facility_source_name').size()
    df_b5_sph  = df_b5[~df_b5.index.isin(df_b5_ph.index)]
    df_b5_sph_wise = df_b5_sph.groupby('bag_facility_source_name').size()
except Exception as E:
    logging.warning(f": Error while Processing the PPPH greater than 12 Aging File:  {E}")


def calculate_and_categorize_time(dataframe, column_name, current_time  , bucket_name):
    df_time_wise = pd.to_datetime(dataframe[column_name])
    df_hour_wise = (current_time - df_time_wise) / pd.Timedelta(hours=1)
    df_hour_wise = df_hour_wise.round().astype(int)
    
    def categorize_time(hour_diff):
        if hour_diff > 48:
            return '> 48 hours'
        elif hour_diff > 24 and hour_diff < 48:
            return '24-48 hours'
        elif hour_diff > 11:
            return '12-24 hours'
    
    dataframe["hour_diff"] = df_hour_wise
    dataframe[bucket_name] = df_hour_wise.apply(categorize_time)
    data = dataframe[bucket_name].value_counts()
    return data


zo_ph_secondary_total = calculate_and_categorize_time(df_secondary_zo_ph , 'fact_updated_at' , current_time , "ZO Secondary Pending PH: ")
zo_sph_secondary_total = calculate_and_categorize_time(df_secondary_zo_sph , 'fact_updated_at' , current_time , "ZO Secondary Pending SPH: ")
print(f"Total ZO Secondary Pending:    {sum(zo_ph_secondary_total) + sum(zo_sph_secondary_total)}")
b5_ph_secondary_total = calculate_and_categorize_time(df_secondary_b5_ph , 'fact_updated_at' , current_time , "B5 Secondary Pending PH: ")
b5_sph_secondary_total = calculate_and_categorize_time(df_secondary_b5_sph , 'fact_updated_at' , current_time , "B5 Secondary Pending SPH: ")
outbond_12_pendency = calculate_and_categorize_time(outbond_12 , 'fact_updated_at' , current_time , "Outbond Pendency")
outbound_12_xd_total = calculate_and_categorize_time(outbound_12_xd , 'fact_updated_at' , current_time , "Outbound Cross Dock")
outbound_12_sl_total = calculate_and_categorize_time(outbound_12_sl , 'fact_updated_at' , current_time , "OB SL Pendency")

zo_ph_bagging_total = calculate_and_categorize_time(df_bagging_zo_ph , 'fact_updated_at' , current_time , "ZO Bagging Pending PH: ")
zo_sph_bagging_total = calculate_and_categorize_time(df_bagging_zo_sph , 'fact_updated_at' , current_time , "ZO Bagging Pending SPH: ")
print(f"Total ZO Secondary Pending:    {sum(zo_ph_bagging_total) + sum(zo_sph_bagging_count)}")
b5_ph_bagging_total = calculate_and_categorize_time(df_bagging_b5_ph , 'fact_updated_at' , current_time , "B5 Bagging Pending PH: ")
b5_sph_bagging_total = calculate_and_categorize_time(df_bagging_b5_sph , 'fact_updated_at' , current_time , "B5 Bagging Pending SPH: ")
print(f"Total B5 Secondary Pending:  {sum(b5_ph_bagging_total) + sum(b5_sph_bagging_total)} ")

final_zo_ph = calculate_and_categorize_time(df_zo_ph , 'fact_updated_at' , current_time , 'ZO PH')
final_zo_sph = calculate_and_categorize_time(df_zo_sph , 'fact_updated_at' , current_time , "ZO SPH")
print(f"ZO Total PH + SPH =   {sum(final_zo_sph) + sum(final_zo_ph)}")
final_b5_ph = calculate_and_categorize_time(df_b5_ph , 'fact_updated_at' , current_time , "B5 PH")
final_b5_sph = calculate_and_categorize_time(df_b5_sph , 'fact_updated_at' , current_time , "B5 SPH")

def listMaker(dataframe1 , dataframe2):
    data = {}
    for index , row in dataframe1.items():
        for index1 , row1 in dataframe2.items():
            if index == index1:
                data[index] = row + row1
    return data

def listMaker1(dataframe1):
    data = {}
    for index , row in dataframe1.items():
        data[index] = row
    return data


    
live_ppph = {"Live_PPPH": ykb_ppph}
live_ph = {"Live PH":  zo_ph_shipment_count + b5_ph_shipment_count}
live_sph = {"Live SPH":  zo_sph_shipment_count + b5_sph_shipment_count}
other_mh_ppph1 = {"Other MH PPPH":  other_mh_count}
ppph_zo_ph = {"ZO PH":  zo_ph_shipment_count}
ppph_b5_ph  = {"B5 PH":  b5_ph_shipment_count}
ppph_zo_sph = {"ZO SPH": zo_sph_shipment_count}
ppph_b5_sph = {"B5 SPH":  b5_sph_shipment_count}
secondary_pending_zo_ph = {"Secondary Pending ZO PH": sum(df_secondary_zo_ph1)}
secondary_pending_b5_ph = {"Secondary Pending B5 PH": sum(df_secondary_b5_ph1)}
secondary_pending_zo_sph = {"Secondary Pending ZO SPH":  sum(df_secondary_zo_sph1)}
secondary_pending_b5_sph  = {"Secondary Pending B5 SPH":  sum(df_secondary_b5_sph1)}
secondary_pending_total_ph = {"Secondary Pending Total PH": sum(df_secondary_zo_ph1) + sum(df_secondary_b5_ph1)}
secondary_pending_total_sph = {"Secondary Pending Total SPH": sum(df_secondary_zo_sph1) + sum(df_secondary_b5_sph1)}
ppph_12_ph  = listMaker(final_zo_ph , final_b5_ph)
ppph_12_sph = listMaker(final_zo_sph , final_zo_sph)
secondary_12_ph = listMaker(zo_ph_secondary_total , b5_ph_secondary_total)
secondary_12_sph = listMaker(zo_sph_secondary_total ,b5_sph_secondary_total)
bagging_pending_zo_ph = {"Bagging Pending ZO PH":  sum(zo_ph_bagging_count)}
bagging_pending_zo_sph = {"Bagging Pending ZO SPH": sum(zo_sph_bagging_count)}
bagging_pending_b5_ph = {"Bagging Pending B5 PH": sum(b5_ph_bagging_count)}
bagging_pending_b5_sph = {"Bagging Pending B5 SPH": sum(b5_sph_bagging_count)}
bagging_pending_total_ph = {"Bagging Pending Total PH": sum(zo_ph_bagging_count) + sum(b5_ph_bagging_count)}
bagging_pending_total_sph = {"Bagging Pending Total SPH":  sum(zo_sph_bagging_count) + sum(b5_sph_bagging_count)}
bagging_12_ph = listMaker(zo_ph_bagging_total , b5_ph_bagging_total)
bagging_12_sph = listMaker(zo_sph_bagging_total , b5_sph_bagging_total)
outbound_total_live = {"Outbound Total Live":  outbound_total}
outbound_sl_live = {"Outbound SL Live": outbound_sl_total}
outbound_xd_live = {"Outbound XD Live": outbound_xd_total}
outbound_total_12_live = listMaker1(outbond_12_pendency)
outbound_total_xd_12_live = listMaker1(outbound_12_xd_total)
outbound_total_sl_12_live = listMaker1(outbound_12_sl_total)

print(f"Live PH: {live_ph}")
print(f"Live SPH: {live_sph}")
print(f"Live Total PPPH: {live_ppph}")
print(f"Other MH:  {other_mh_ppph1}")
print(f"PPPH ZO PH: {ppph_zo_ph}")
print(f"PPPH B5 PH: {ppph_b5_ph}")
print(f"PPPH ZO SPH: {ppph_zo_sph}")
print(f"PPPH B5 SPH: {ppph_b5_sph}")
print(f"PPPH PH Ageing: {ppph_12_ph}")
print(f"PPPH SPH Ageing: {ppph_12_sph}")
print(secondary_pending_zo_ph)
print(secondary_pending_b5_ph)
print(secondary_pending_zo_sph)
print(secondary_pending_b5_sph)
print(secondary_pending_total_ph)
print(secondary_pending_total_sph)
print(f"Secondary Pending PH 12:  {secondary_12_ph}")
print(f"Secondary Pending SPH 12:  {secondary_12_sph}")
print(bagging_pending_zo_ph)
print(bagging_pending_zo_sph)
print(bagging_pending_b5_ph)
print(bagging_pending_b5_sph)
print(bagging_pending_total_ph)
print(bagging_pending_total_sph)
print(f"Bagging Pending PH 12: {bagging_12_ph}")
print(f"Bagging Pending SPH 12: {bagging_12_sph}")
print(outbound_total_live)
print(outbound_sl_live)
print(outbound_xd_live)
print(f"Outbound 12: {outbound_total_12_live}")
print(f"Outbound 12 XD: {outbound_total_xd_12_live}")
print(f"Outbound 12 SL:  {outbound_total_sl_12_live}")

