import pandas as pd
from datetime import datetime
import pytz
import logging
import pymysql


time_zone = pytz.timezone('UTC')
current_time= datetime.now(time_zone)
file_path = "/home/administrator/cbs_bag_hold/data/"

# logging.basicConfig(format='%(asctime)s %(message)s' , datefmt='%m/%d/%Y %I:%M:%S %p' , filename='auto_pendency.logs' , encoding='utf-8' , level=logging.DEBUG )

logging.basicConfig(format='%(asctime)s %(message)s' , datefmt='%m/%d/%Y %I:%M:%S %p' , filename='auto_pendency.logs' , level=logging.DEBUG )

conn = pymysql.connect(
    host='localhost',
    user='abhishek',
    password='abhi',
    db='pendency',
    charset='utf8mb4',
    cursorclass=pymysql.cursors.DictCursor
)

multi_value_sql_tables = ["live_values" , "ppph_12" , "ppph_12_zo" , "ppph_12_b5" , "secondary_12_zo" , 'secondary_12_b5' , 'bagging_12_zo' , 'bagging_12_b5' , "outbound_total_12_live" , "outbound_total_xd_12_live" , "outbound_total_sl_12_live" , 'other_mh_12' , "secondary_12" , 'bagging_12']
              
single_value_sql_table = ["live_ph" , 'live_sph' , "live_ppph" , "ppph_zo_ph" , "ppph_b5_ph" , "ppph_zo_sph" , "ppph_b5_sph" , "other_mh_ppph1" ,  "secondary_pending_zo_ph" , "secondary_pending_zo_sph", "secondary_pending_b5_ph" , "secondary_pending_b5_sph" , "secondary_pending_total_ph" , 'secondary_pending_total_sph' , "bagging_pending_zo_ph" , "bagging_pending_zo_sph" , "bagging_pending_b5_ph" , "bagging_pending_b5_sph" , "bagging_pending_total_ph" ,  "bagging_pending_total_sph" , "outbound_total_live" , "outbound_sl_live" ,  "outbound_xd_live" , "zo_ppph_total" , "b5_ppph_total" , 'other_mh_PH' , "other_mh_SPH" , "other_mh_Total"  ,"total_secondary_pending" , 'total_secondary_pending_zo' , "total_secondary_pending_b5" , "total_bagging_pending" , "total_bagging_pending_zo" , "total_bagging_pending_b5"]

def single_value_table_creator():
    try:
        with conn.cursor() as cursor:
            for table in single_value_sql_table:
                sql = f"CREATE TABLE IF NOT EXISTS {table} (datetime DATETIME , bucket VARCHAR(255) , count INT)"
                cursor.execute(sql)
        conn.commit()
        print("Sql Table Created")
    except Exception as e:
        logging.warning(f"Error in Creating Table:  {e}")
    # finally:
    #     conn.close()

def multi_value_table_creator():
    try:
        with conn.cursor() as cursor:
            for table in multi_value_sql_tables:
                sql = f"CREATE TABLE IF NOT EXISTS {table} (datetime DATETIME , bucket VARCHAR(255) , 12_24 INT , 24_48 INT , gt_48 INT)"
                cursor.execute(sql)
            conn.commit()
        print("Multi Value Table Created")
    except Exception as E:
        logging.warning(f"Error in Creating Mutli Value Table: {E}")
 
single_value_table_creator()
multi_value_table_creator()


def table_exists():
    query = f"SHOW TABLES"
    with conn.cursor() as cursor:
        cursor.execute(query)
        result = cursor.fetchall()
        return result


try:
    df_secondary = pd.read_csv(f'{file_path}ykb_secondary_pending_abhi.csv')
except Exception as E:
    logging.warning(f"Error Secondary File not found:  {E}")
try:
    outbond_df = pd.read_csv(f'{file_path}ykb_outbound.csv')
except Exception as E:
    logging.warning(f"Error Outbound file not Found: {E}")
try:
    outbond_12 = pd.read_csv(f"{file_path}ykb_outbond_pending_greater_than_12.csv")
except Exception as E:
    logging.warning(f"Error Outbound12 File not found {E}")
try:
    df_ppph1 = pd.read_csv(f'{file_path}ykb_pendency_automation_PPPH.csv')
except Exception as E:
    logging.warning(f"Error PPPH File not Found {E}")
try:
    df_bagging = pd.read_csv(f'{file_path}ykb_bagging_pending_automation_abhi.csv')
except Exception as E:
    logging.warning(f"Error while reading the Bagging File {E}")
try:
    df = pd.read_csv(f"{file_path}Pendency_automation_report_ageing_greater_than_12.csv")
except Exception as E:
    logging.warning(f"Error while reading PPPH12 File {E}")
try:
    outbond_xd = pd.read_csv(f'{file_path}ykb_outbond_crossdock_abhi.csv')
except Exception as E:
    logging.warning(f"Error while Reading the Outbond XD file {E}")
try:
    outbond_sl = pd.read_csv(f'{file_path}outbond_semi_large_abhi.csv')
except Exception as E:
    logging.warning(f"Error Outbond SL File not found {E}")
try:
    outbound_12_xd = pd.read_csv(f"{file_path}ykb_outbound_xd_greater_than_12.csv")
except Exception as E:
    logging.warning(F"Error while reading Outbound XD 12 File: {E}")
try:
    outbound_12_sl = pd.read_csv(f"{file_path}ykb_outbound_sl_greater_than-12.csv")
    # Spelling Mistake
except Exception as E:
    logging.warning(f"Error while reading Outbound SL 12 File  {E}")
try:
    df_for_OMH = pd.read_csv(f"{file_path}Pendency_automation_report_ageing_greater_than_12.csv")
except Exception as E:
    logging.warning(f"Error {E}")



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
    other_mh_ph = other_mh[other_mh['bag_facility_source_name'].str.contains("DEL_PL|Bilaspur|Bhiwani|Bhiwadi")]
    other_mh_ph_count = other_mh_ph['tracking_id_ekart'].sum()
    other_mh_sph = other_mh[~other_mh.index.isin(other_mh_ph.index)]
    other_mh_sph_count = other_mh_sph['tracking_id_ekart'].sum()

except Exception as E:
    logging.debug(f": Error while Processing the PPPH Files : {E}")

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
    df = df[df['bag_facility_current_name'] == "MotherHub_YKB"]
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

try:
    df_mh_ykb = df_for_OMH[df_for_OMH['bag_facility_current_name'] == "MotherHub_YKB"]
    df_other_mh = df_for_OMH[~df_for_OMH.index.isin(df_mh_ykb.index)]
except Exception as E:
    logging.warning(E)


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

try:
    zo_ph_secondary_total = calculate_and_categorize_time(df_secondary_zo_ph , 'fact_updated_at' , current_time , "ZO Secondary Pending PH: ")
except Exception as E:
    logging.warning(E)
try:
    zo_sph_secondary_total = calculate_and_categorize_time(df_secondary_zo_sph , 'fact_updated_at' , current_time , "ZO Secondary Pending SPH: ")
except Exception as E:
    logging.warning(E)
try:
    b5_ph_secondary_total = calculate_and_categorize_time(df_secondary_b5_ph , 'fact_updated_at' , current_time , "B5 Secondary Pending PH: ")
except Exception as E:
    logging.warning(E)
try:
    b5_sph_secondary_total = calculate_and_categorize_time(df_secondary_b5_sph , 'fact_updated_at' , current_time , "B5 Secondary Pending SPH: ")
except Exception as E:
    logging.warning(E)
try:
    outbond_12_pendency = calculate_and_categorize_time(outbond_12 , 'fact_updated_at' , current_time , "Outbond Pendency")
except Exception as E:
    logging.warning(E)
try:
    outbound_12_xd_total = calculate_and_categorize_time(outbound_12_xd , 'fact_updated_at' , current_time , "Outbound Cross Dock")
except Exception as E:
    logging.warning(E)
try:
    outbound_12_sl_total = calculate_and_categorize_time(outbound_12_sl , 'fact_updated_at' , current_time , "OB SL Pendency")
except Exception as E:
    logging.warning(E)
try:
    zo_ph_bagging_total = calculate_and_categorize_time(df_bagging_zo_ph , 'fact_updated_at' , current_time , "ZO Bagging Pending PH: ")
except Exception as E:
    logging.warning(E)
try:
    zo_sph_bagging_total = calculate_and_categorize_time(df_bagging_zo_sph , 'fact_updated_at' , current_time , "ZO Bagging Pending SPH: ")
except Exception as E:
    logging.warning(E)
try:
    b5_ph_bagging_total = calculate_and_categorize_time(df_bagging_b5_ph , 'fact_updated_at' , current_time , "B5 Bagging Pending PH: ")
except Exception as E:
    logging.warning(E)
try:
    b5_sph_bagging_total = calculate_and_categorize_time(df_bagging_b5_sph , 'fact_updated_at' , current_time , "B5 Bagging Pending SPH: ")
except Exception as E:
    logging.warning(E)
try:
    final_zo_ph = calculate_and_categorize_time(df_zo_ph , 'fact_updated_at' , current_time , 'ZO PH')
except Exception as E:
    logging.warning(E)
try:
    final_zo_sph = calculate_and_categorize_time(df_zo_sph , 'fact_updated_at' , current_time , "ZO SPH")
except Exception as E:
    logging.warning(E)
try:
    final_b5_ph = calculate_and_categorize_time(df_b5_ph , 'fact_updated_at' , current_time , "B5 PH")
except Exception as E:
    logging.warning(E)
try:
    final_b5_sph = calculate_and_categorize_time(df_b5_sph , 'fact_updated_at' , current_time , "B5 SPH")
except Exception as E:
    logging.warning(E)
try:
    other_mh_df = calculate_and_categorize_time(df_other_mh , 'fact_updated_at' , current_time , "Other MH Ageing")
except Exception as E:
    logging.warning(E)


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
other_mh_PH = {"Other MH PH": other_mh_ph_count}
other_mh_SPH = {"Other MH SPH": other_mh_sph_count}
other_mh_Total = {"Other MH Total": other_mh_ph_count + other_mh_sph_count}
secondary_pending_zo_ph = {"Secondary Pending ZO PH": sum(df_secondary_zo_ph1)}
secondary_pending_b5_ph = {"Secondary Pending B5 PH": sum(df_secondary_b5_ph1)}
secondary_pending_zo_sph = {"Secondary Pending ZO SPH":  sum(df_secondary_zo_sph1)}
secondary_pending_b5_sph  = {"Secondary Pending B5 SPH":  sum(df_secondary_b5_sph1)}
secondary_pending_total_ph = {"Secondary Pending Total PH": sum(df_secondary_zo_ph1) + sum(df_secondary_b5_ph1)}
secondary_pending_total_sph = {"Secondary Pending Total SPH": sum(df_secondary_zo_sph1) + sum(df_secondary_b5_sph1)}
total_secondary_pending = {"Secondary Pending Total": sum(df_secondary_zo_ph1) + sum(df_secondary_b5_ph1) + sum(df_secondary_zo_sph1) + sum(df_secondary_b5_sph1)}
total_secondary_pending_zo = {"Secondary Pending ZO": sum(df_secondary_zo_ph1) + sum(df_secondary_zo_sph1)}
total_secondary_pending_b5 = {"Secondary Pending B5": sum(df_secondary_b5_ph1) + sum(df_secondary_b5_sph1)}
ppph_12_zo  = listMaker(final_zo_ph , final_zo_sph)
ppph_12_b5 = listMaker(final_b5_ph , final_b5_sph)
ppph_12 = listMaker(ppph_12_zo , ppph_12_b5)
secondary_12_zo = listMaker(zo_ph_secondary_total , zo_sph_secondary_total)
secondary_12_b5 = listMaker(b5_ph_secondary_total ,b5_sph_secondary_total)
secondary_12 = listMaker(secondary_12_zo , secondary_12_b5)
bagging_pending_zo_ph = {"Bagging Pending ZO PH":  sum(zo_ph_bagging_count)}
bagging_pending_zo_sph = {"Bagging Pending ZO SPH": sum(zo_sph_bagging_count)}
bagging_pending_b5_ph = {"Bagging Pending B5 PH": sum(b5_ph_bagging_count)}
bagging_pending_b5_sph = {"Bagging Pending B5 SPH": sum(b5_sph_bagging_count)}
bagging_pending_total_ph = {"Bagging Pending Total PH": sum(zo_ph_bagging_count) + sum(b5_ph_bagging_count)}
bagging_pending_total_sph = {"Bagging Pending Total SPH":  sum(zo_sph_bagging_count) + sum(b5_sph_bagging_count)}
total_bagging_pending = {"Total Bagging pending": sum(zo_ph_bagging_count)+ sum(zo_sph_bagging_count) + sum(b5_ph_bagging_count) + sum(b5_sph_bagging_count)}
total_bagging_pending_zo = {"Total ZO Bagging Pending": sum(zo_ph_bagging_count) + sum(zo_sph_bagging_count)}
total_bagging_pending_b5 = {"Total B5 Bagging Pending": sum(b5_ph_bagging_count) + sum(b5_sph_bagging_count)}
bagging_12_zo = listMaker(zo_ph_bagging_total , zo_sph_bagging_total)
bagging_12_b5 = listMaker(b5_ph_bagging_total , b5_sph_bagging_total)
bagging_12 = listMaker(bagging_12_zo , bagging_12_b5)
outbound_total_live = {"Outbound Total Live":  outbound_total}
outbound_sl_live = {"Outbound SL Live": outbound_sl_total}
outbound_xd_live = {"Outbound XD Live": outbound_xd_total}
outbound_total_12_live = listMaker1(outbond_12_pendency)
outbound_total_xd_12_live = listMaker1(outbound_12_xd_total)
outbound_total_sl_12_live = listMaker1(outbound_12_sl_total)
zo_ppph_total = {"ZO PPPH Total": zo_ph_shipment_count + zo_sph_shipment_count}
b5_ppph_total = {"B5 PPPH Total": b5_ph_shipment_count + b5_sph_shipment_count}
other_mh_12 = listMaker1(other_mh_df)

print(f"Live PH: {live_ph}")
print(f"Live SPH: {live_sph}")
print(f"Live Total PPPH: {live_ppph}")
print(f"Other MH:  {other_mh_ppph1}")
print(f"PPPH ZO PH: {ppph_zo_ph}")
print(f"PPPH B5 PH: {ppph_b5_ph}")
print(f"PPPH ZO SPH: {ppph_zo_sph}")
print(f"PPPH B5 SPH: {ppph_b5_sph}")
print(secondary_pending_zo_ph)
print(secondary_pending_b5_ph)
print(secondary_pending_zo_sph)
print(secondary_pending_b5_sph)
print(secondary_pending_total_ph)
print(secondary_pending_total_sph)
print(bagging_pending_zo_ph)
print(bagging_pending_zo_sph)
print(bagging_pending_b5_ph)
print(bagging_pending_b5_sph)
print(bagging_pending_total_ph)
print(bagging_pending_total_sph)
print(outbound_total_live)
print(outbound_sl_live)
print(outbound_xd_live)
print(f"PPPH_PH_12: {ppph_12_zo}")
print(f"PPPH SPH A12: {ppph_12_b5}")
print(f"Secondary Pending PH 12:  {secondary_12_zo}")
print(f"Secondary Pending SPH 12:  {secondary_12_b5}")
print(f"Bagging Pending PH 12: {bagging_12_zo}")
print(f"Bagging Pending SPH 12: {bagging_12_b5}")
print(f"Outbound 12: {outbound_total_12_live}")
print(f"Outbound 12 XD: {outbound_total_xd_12_live}")
print(f"Outbound 12 SL:  {outbound_total_sl_12_live}")



def dict_to_sql(item, table_name):
    try:
        with conn.cursor() as cursor:
            for key, value in item.items():
                insert_query = """
                    INSERT INTO {table_name} (datetime, bucket, count)
                    VALUES (%s , %s, %s)
                """.format(table_name=table_name)
                cursor.execute(insert_query, (current_time , key , value))
            conn.commit()
    except Exception as e:
        print(f"Error: {e}")
        conn.rollback()  

def ageing_to_sql(item, table_name):
    try:
        with conn.cursor() as cursor:
            keys = ['12-24 hours', '24-48 hours', '> 48 hours']
            values = {'hours_12_24': 0, 'hours_24_48': 0, 'hours_gt_48': 0}
            for key in keys:
                if key == '12-24 hours':
                    values['12_24'] = item.get(key, 0)
                elif key == '24-48 hours':
                    values['24_48'] = item.get(key, 0)
                elif key == '> 48 hours':
                    values['gt_48'] = item.get(key, 0)
            insert_query = f"INSERT INTO {table_name} (datetime, bucket, 12_24, 24_48, gt_48) VALUES (%s, %s, %s, %s, %s)"
            cursor.execute(insert_query, (current_time, {table_name}, values['12_24'], values['24_48'], values['gt_48']))
            conn.commit()
    except Exception as e:
        print(f"Error: {e}")
        conn.rollback()

dict_to_sql(zo_ppph_total , "zo_ppph_total")
dict_to_sql(b5_ppph_total , "b5_ppph_total")
dict_to_sql(other_mh_PH , 'other_mh_PH')
dict_to_sql(other_mh_SPH , 'other_mh_SPH')
dict_to_sql(other_mh_Total , 'other_mh_Total')
dict_to_sql(live_ph , 'live_ph')
dict_to_sql(live_sph , 'live_sph')
dict_to_sql(live_ppph , "live_ppph")
dict_to_sql(ppph_zo_ph , "ppph_zo_ph")
dict_to_sql(ppph_b5_ph , "ppph_b5_ph")
dict_to_sql(ppph_zo_sph , "ppph_zo_sph")
dict_to_sql(ppph_b5_sph , "ppph_b5_sph") 
dict_to_sql(other_mh_ppph1 , "other_mh_ppph1")
dict_to_sql(secondary_pending_zo_ph , "secondary_pending_zo_ph")
dict_to_sql(secondary_pending_b5_ph , 'secondary_pending_b5_ph')
dict_to_sql(secondary_pending_zo_sph , "secondary_pending_zo_sph")
dict_to_sql(secondary_pending_b5_sph , 'secondary_pending_b5_sph')
dict_to_sql(secondary_pending_total_ph , "secondary_pending_total_ph")
dict_to_sql(secondary_pending_total_sph , "secondary_pending_total_sph")
dict_to_sql(total_secondary_pending , 'total_secondary_pending')
dict_to_sql(total_secondary_pending_zo, "total_secondary_pending_zo")
dict_to_sql(total_secondary_pending_b5 , "total_secondary_pending_b5")
dict_to_sql(total_bagging_pending , "total_bagging_pending")
dict_to_sql(total_bagging_pending_zo , "total_bagging_pending_zo")
dict_to_sql(total_bagging_pending_b5 , "total_bagging_pending_b5")
dict_to_sql(bagging_pending_zo_ph , 'bagging_pending_zo_ph')
dict_to_sql(bagging_pending_zo_sph , "bagging_pending_zo_sph")
dict_to_sql(bagging_pending_b5_ph , "bagging_pending_b5_ph")
dict_to_sql(bagging_pending_b5_sph , "bagging_pending_b5_sph")
dict_to_sql(bagging_pending_total_ph , "bagging_pending_total_ph") 
dict_to_sql(bagging_pending_total_sph , 'bagging_pending_total_sph')
dict_to_sql(outbound_total_live , 'outbound_total_live')
dict_to_sql(outbound_sl_live , 'outbound_sl_live')
dict_to_sql(outbound_xd_live , "outbound_xd_live")
ageing_to_sql(ppph_12 , "ppph_12")
ageing_to_sql(ppph_12_zo , "ppph_12_zo")
ageing_to_sql(ppph_12_b5 , "ppph_12_b5")
ageing_to_sql(secondary_12_zo , "secondary_12_zo")
ageing_to_sql(secondary_12_b5 , 'secondary_12_b5')
ageing_to_sql(bagging_12_zo , 'bagging_12_zo')
ageing_to_sql(bagging_12_b5 , 'bagging_12_b5')
ageing_to_sql(outbound_total_12_live , 'outbound_total_12_live')
ageing_to_sql(outbound_total_xd_12_live , 'outbound_total_xd_12_live')
ageing_to_sql(outbound_total_sl_12_live  , 'outbound_total_sl_12_live')
ageing_to_sql(other_mh_12 , 'other_mh_12')
ageing_to_sql(secondary_12 , 'secondary_12')
ageing_to_sql(bagging_12 , 'bagging_12')
print(table_exists())
conn.close()