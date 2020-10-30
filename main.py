#-*- coding: utf-8 -*-

import os
import sys
import time
import argparse
import pandas as pd
import numpy as np
from datetime import date
from datetime import datetime as ddtt
from collections import defaultdict
import datetime
import gspread
from google.cloud import storage
from google.cloud import datastore
import google.cloud.exceptions
from gspread_pandas import Spread, Client
from oauth2client.service_account import ServiceAccountCredentials
from urllib.parse import urlparse, urlencode, parse_qs
from youtube.videos_channelid import ChannelVideo
from youtube.search_keyword import searchVideo
from youtube.video_tags import VideoTags
from utils.send_sms import SendSms
from config import SAVE_PATH
# import cloudstorage as gcs


def read_file(filename):
    with open(filename) as fp:
        return fp.read()


def write_file(filename, last_row):
    with open(filename, 'w') as fp:
        print('The number of row to save to file is : ' + str(last_row))
        fp.write(str(last_row))


def upsert(client, email_with_telephone, user_unique_id, danone_kind_use):
    # [START datastore_upsert]
    # 'sample_task'

    with client.transaction():
        complete_key = client.key(danone_kind_use,  email_with_telephone)

        task = datastore.Entity(key=complete_key)

        task.update({
            'danone_uid': user_unique_id
        })

        client.put(task)
    # [END datastore_upsert]

    return task


def lookup(client, danone_kind_use, email_with_telephone):
    # Create the entity that we're going to look up.
    # upsert(client)

    # [START datastore_lookup]
    key = client.key(danone_kind_use, email_with_telephone)
    task = client.get(key)
    # [END datastore_lookup]

    return task


def generate_danone_uid_specific_to_group(ds_client, danone_grp, e_w_t, current_id):
    # danone_grp is Danone_main
    #
    ds_en_exist = lookup(ds_client, danone_grp, e_w_t)

    if ds_en_exist:
        print("Key already exist")
        print("The valud of uid is " + str(ds_en_exist['danone_uid']))
        return ds_en_exist['danone_uid']
    else:
        t_current_id = current_id + 1
        upsert(ds_client, e_w_t, t_current_id, danone_grp)
        return t_current_id

#    pass

def main():
    os.makedirs("output", exist_ok=True)
    parser = argparse.ArgumentParser()

    if str(sys.argv[1]) == "--sc":
        parser.add_argument("--sc", help="calls the search by channel by keyword function", action='store_true')
        parser.add_argument("--gsfile", help="google sheet file", required=True)
        parser.add_argument("--gcsfile", help="google cloud storage for storing running number", required=True)
        parser.add_argument("--kindname", help="kind use for store Danone data", required=True)
        parser.add_argument("--sheetnum", help="please provide sheet number", required=True)
        parser.add_argument("--destsheet", help="enter destination sheet", required=True)
        parser.add_argument("--isSendSMS", help="Need to send SMS or not", required=False)
        parser.add_argument("--shortLinkCol", help="Enter shortlink column position", required=False)
        parser.add_argument("--startRow", help="Enter start row number", required=False)
        parser.add_argument("--endRow", help="Enter end row number", required=False)
        parser.add_argument("--textForSMS", help="Enter text to send with SMS", required=False)
        parser.add_argument("--urlForShort", help="Enter the url for shorten link service", required=False)

        args = parser.parse_args()

        toSend_sms = int(args.isSendSMS)

        sms_obj = SendSms('HiFamily', 'analytics', 'predictive2020P_')
    #   sms_obj = SendSms('HiFamily', 'analytics', 'predictive2020P_')


        if toSend_sms == 1:
            sms_obj.send_msg('ทดสอบภาษาไทย at rebrand.ly/owzupsp', '0909568716')
            # exit(0)

        scope = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']

        creds = ServiceAccountCredentials.from_json_keyfile_name(
            'profile/Predictive-e9f9724b4813.json', scope)

        client = gspread.authorize(creds)

        gcs_clit = storage.Client.from_service_account_json('profile/Predictive-e9f9724b4813.json')

        danone_k_name = args.kindname
        sh_number = int(args.sheetnum)

        # Instantiates a client
        datastore_client = datastore.Client()

        # End initial datastore object
        # get bucket with name
        bucket = gcs_clit.get_bucket('youtube-poc-data')
        # get bucket data as blob
        blob = bucket.get_blob(args.gcsfile)
        # convert to string
        txt_data = blob.download_as_string()

        today = date.today()
        midnight = ddtt.combine(today, ddtt.min.time())
        yesterday_midnight = midnight - datetime.timedelta(days=1)

        print(yesterday_midnight)
        # cv = ChannelVideo(args.channelid, args.max, args.key, yesterday_midnight)

        # cv.get_channel_videos()
        # Next step is read from the csv file into dataframe
        # youtube_rawfin_data = pd.read_csv(SAVE_PATH + "search_channel_id.csv", header=None, usecols=[1, 3],
        #                                names=['video_title', 'video_id'], sep=',')

        # slicing_data = youtube_fin_data.iloc[:, [1, 3]].copy()

        # youtube_fin_data = youtube_rawfin_data.loc[(youtube_rawfin_data["video_title"].str.contains("FIN", case=True)) |
        #                                            (youtube_rawfin_data["video_title"].str.contains("EP", case=True))]

        # print(youtube_fin_data)

        # final_combine_data = defaultdict(list)

        # youtube_dict_partial_data = youtube_fin_data.to_dict('list')

        # for tmp in youtube_dict_partial_data.keys():
        #     print(tmp)
        #     for each_item in youtube_dict_partial_data[tmp]:
        #         final_combine_data[tmp].append(each_item)
        #         if tmp == 'video_id':
        #             vt = VideoTags(args.max, each_item, args.key)
        #             real_tag, real_view_count = vt.get_video_tag()
        #             final_combine_data["video_tags"].append(real_tag)
        #             final_combine_data["video_view_count"].append(real_view_count)
                    # get the tag
                    # next step is to get tag for each iterate video

        # print(final_combine_data)

        # result_df = pd.DataFrame().from_dict(final_combine_data)

        # result_df.columns = ['video_title', 'video_id', 'video_tags', 'video_view_count']

        # result_sorted_df = result_df.sort_values(['video_tags', 'video_title'])

        # result_sorted_df.to_csv(SAVE_PATH + "video_id_with_tag.csv", header=False)

        # next step write the data back to google sheet
        # maybe we want to write the file into google cloud storage too
        # first write the header first

        # last_save_total_row = read_file(SAVE_PATH + "total_row.txt")
        # convert into integer
        # in the first time last_save_total_row will equal to one

        last_save_total_row = int(txt_data)
        beginning_row = last_save_total_row

        sh = client.open(args.gsfile)

        # worksheet_danone = sh.get_worksheet(0)
        # list_of_lists0 = worksheet_danone.get_all_values()
        # column_names = list_of_lists0.pop(0)
        #worksheet_danone1 = sh.get_worksheet(1)
        #list_of_lists1 = worksheet_danone1.get_all_values()
        #worksheet_danone2 = sh.get_worksheet(2)
        #list_of_lists2 = worksheet_danone2.get_all_values()
        #worksheet_danone3 = sh.get_worksheet(3)
        #list_of_lists3 = worksheet_danone3.get_all_values()
        # pregnancy = pd.DataFrame(list_of_lists0, columns=column_names)
        #baby0_6m = pd.DataFrame(list_of_lists1, columns=column_names)
        #baby7_12m = pd.DataFrame(list_of_lists2, columns=column_names)
        #baby1year = pd.DataFrame(list_of_lists3, columns=column_names)
        #baby0_6m1 = baby0_6m.loc[baby0_6m['Phone Number'] != '']
        #baby7_12m1 = baby7_12m.loc[baby7_12m['Phone Number'] != '']
        #baby1year1 = baby1year.loc[baby1year['Phone Number'] != '']
        #baby0_6m1.drop_duplicates(subset='Phone Number')
        #baby7_12m1.drop_duplicates(subset='Phone Number')
        #baby1year1.drop_duplicates(subset='Phone Number')
        
        #pregnancy_email = pregnancy.loc[pregnancy['Phone Number'] == '']
        #pregnancy_email.drop_duplicates(subset='Email')
        #baby0_6m_email = baby0_6m.loc[baby0_6m['Phone Number'] == '']
        #baby0_6m_email.drop_duplicates(subset='Email')
        #baby7_12m_email = baby7_12m.loc[baby7_12m['Phone Number'] == '']
        #baby7_12m_email.drop_duplicates(subset='Email')
        #baby1year_email = baby1year.loc[baby1year['Phone Number'] == '']
        #baby1year_email.drop_duplicates(subset='Email')
        
        #phone1 = len(pregnancy1.index)
        #phone2 = len(baby0_6m1.index)
        #phone3 = len(baby7_12m1.index)
        #phone4 = len(baby1year1.index)
        #email1 = len(pregnancy_email.index)
        #email2 = len(baby0_6m_email.index)
        #email3 = len(baby7_12m_email.index)
        #email4 = len(baby1year_email.index)
        
        #print("List of Phone number = "+phone1+phone2+phone3+phone4)

        #print("List of Email = "+email1+email2+email3+email4)

        worksheet_danone = sh.get_worksheet(sh_number)                      # the old value is 1 

        list_of_lists = worksheet_danone.get_all_values()

        column_names = list_of_lists.pop(0)

        danone_df = pd.DataFrame(list_of_lists, columns=column_names)
        # print(danone_df)
        # df1 = danone_df.copy().drop_duplicates(subset=['Email', 'Phone_number'], keep='first')
        if toSend_sms == 0:
            # clean Nan
            danone_clean_df = danone_df.replace(to_replace=np.nan, value='')

            df_tmp = danone_clean_df.loc[danone_clean_df['Phone Number'] != '']

            df1 = df_tmp.drop_duplicates(subset='Phone Number')

            # df1['unique'] = df1['Email'] + "_" + df1['Phone_number'].astype(str)

            # before = len(danone_df.index)
            # after = len(df1.index)
            # duplicates = before - after
            # print("{} {} {}".format("There are ", duplicates, " duplicate rows"))

            danone_uid_arr = []
            danone_long_url = []
            short_url_placeholder = []
            sms_thai_message = []
            my_website_use = args.urlForShort + '?customer_id={:>06d}'

            for colu in df1[['Phone Number']]:
                columnSeriesObj = df1[colu]
                tmp_arr = columnSeriesObj.values
                print("the unique all value is : ", tmp_arr)
                for vv in tmp_arr:
                    temp_id = generate_danone_uid_specific_to_group(datastore_client, danone_k_name, vv, beginning_row)
                    if temp_id > beginning_row:
                        beginning_row = temp_id
                
                    without_pad_str = str(temp_id)
                    full_path_web_url = my_website_use.format(temp_id) 
                    danone_uid_arr.append(without_pad_str)   # original code is : str(temp_id))
                    short_url_placeholder.append(" ")
                    sms_thai_message.append(args.textForSMS)
                    danone_long_url.append(full_path_web_url)



            # for u_id in danone_uid_arr:
            #     print("uid val ", u_id)

            final_clean_df = df1.assign(unique_uid = danone_uid_arr, msg_to_send = sms_thai_message, full_uiq_path = danone_long_url, Short_url = short_url_placeholder)

            spread = Spread('Danone_clean')

            spread.sheets

            spread.df_to_sheet(final_clean_df, index=False, sheet=args.destsheet, start='A1', replace=True)

            print(spread)
        else:
            # Send sms message here
            row_coutr = 1
            short_link_column = args.shortLinkCol
            smsTxt = args.textForSMS
            st_row = int(args.startRow)
            end_row = int(args.endRow)

            for index, row in danone_df.iterrows():
                # c_series_obj = danone_df[cu]
                # tmpList = c_series_obj.values
                # for v in tmpList:
                if row_coutr >= st_row and row_coutr <= end_row:
                    phone_n = row['Phone Number']
                    short_l = row[short_link_column]

                    tts_sms = smsTxt + " " + short_l
                    # next step is to send sms krub
                    sms_obj.send_msg(tts_sms, phone_n)
                    time.sleep(1)                                    
                    

                row_coutr = row_coutr + 1

            # pass

        # if last_save_total_row == 1:
        #     beginning_row = beginning_row + 1
        #     worksheet_youtube.update_cell(1, 1, 'Video Title')
        #     worksheet_youtube.update_cell(1, 2, 'Video id')
        #     worksheet_youtube.update_cell(1, 3, 'Video first tag')
        #     worksheet_youtube.update_cell(1, 4, 'Video URL')
        #     worksheet_youtube.update_cell(1, 5, 'Number of views')
        # row_cnt = beginning_row                                             # 2
        # firstly write the video title to the google sheet
        # for each_video_title in result_df.iloc[:, 0]:
        #     worksheet_youtube.update_cell(row_cnt, 1, each_video_title)
        #     time.sleep(1)
        #     row_cnt = row_cnt + 1
        # Video ID
        # time.sleep(100)
        # row_cnt = beginning_row                                             # 2
        # for each_video_id in result_df.iloc[:, 1]:
        #     worksheet_youtube.update_cell(row_cnt, 2, each_video_id)
        #     time.sleep(1)
        #     worksheet_youtube.update_cell(row_cnt, 4, "http://www.youtube.com/watch?v=" + each_video_id)
        #     time.sleep(1)
        #     row_cnt = row_cnt + 1
        # Video Tag
        # time.sleep(100)
        # row_cnt = beginning_row                                             # 2
        # for each_video_tagging in result_df.iloc[:, 2]:
        #     worksheet_youtube.update_cell(row_cnt, 3, each_video_tagging)
        #     time.sleep(1)
        #     row_cnt = row_cnt + 1
        # Video View Count
        # time.sleep(100)
        # row_cnt = beginning_row                                             # 2
        # for each_video_view_count in result_df.iloc[:, 3]:
        #     worksheet_youtube.update_cell(row_cnt, 5, each_video_view_count)
        #     time.sleep(1)
        #     row_cnt = row_cnt + 1
        # At here we need to write to the file on GCS
        # Write data and time of retrieve data into Google Sheet
        # worksheet_youtube.update_cell(row_cnt, 1, yesterday_midnight.strftime('%m/%d/%Y'))
        # row_cnt = beginning_row + 1

        blob.upload_from_string(str(beginning_row))


        # row_cnt will be the number of row in the google sheet
        # write_file(SAVE_PATH + "total_row.txt", row_cnt)
        # Check whether we can delete the row or not
        # time.sleep(100)
        # if row_cnt < last_save_total_row:
        #    var i : int
        #    i = row_cnt
        #    while i < last_save_total_row:
        #        print(i)
        #        worksheet_youtube.delete_row(i)
        #        time.sleep(1)
        #        i = i + 1

        exit(0)
        
    else:
        print("Invalid Arguments")
        exit(1)


if __name__ == '__main__':
    main()
