# -*- coding: utf-8 -*-

import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import os
import re

class UFCScraper():

    def __init__(self):
        #self.script_dir = os.path.dirname(__file__) + '../'
        self.data = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'data'))
        self.fight_path = "/fight_stats.csv"
        self.fighter_path = "/fighter_stats.csv"
        self.http = self.create_retries()

    def create_retries(self):
        retry_strategy = Retry(
            total=5,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "OPTIONS"],
            backoff_factor=2
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        http = requests.Session()
        http.mount("https://", adapter)
        http.mount("http://", adapter)
        return http

    # def get_abs_path(self, rel_path):
    #     abs_file_path_hist = os.path.join(self.script_dir, rel_path)
    #     return abs_file_path_hist

    def scrape_fights(self):
        # Update fights
        fight_stats = self.data + self.fight_path
        fighter_stats = self.data + self.fighter_path

        fight_stats_updated, fighter_stats_updated = self.update(fight_stats, fighter_stats)
        self.write_fights(fight_stats_updated, fighter_stats_updated, fight_stats, fighter_stats)

        return fighter_stats_updated, fight_stats_updated

    def update(self, fight_stats, fighter_stats):
        """
        Returns the updated fight history and fighter stats dataframes
        """
        # get old data
        if os.path.exists(fight_stats):
            fight_stats_old = pd.read_csv(fight_stats)
            fighter_stats_old = pd.read_csv(fighter_stats)
        else:
            fight_cols = ["date","fight_url","event_url","result","last_5_comps_per_event","fighter","opponent","division","method","round","time",
                    "fighter_url","opponent_url","referee", "time_format", "knockdowns","sub_attempts","reversals","control","takedowns_landed",
                    "takedowns_attempts","sig_strikes_landed","sig_strikes_attempts","total_strikes_landed",
                    "total_strikes_attempts","head_strikes_landed","head_strikes_attempts","body_strikes_landed",
                    "body_strikes_attempts","leg_strikes_landed","leg_strikes_attempts","distance_strikes_landed",
                    "distance_strikes_attempts","clinch_strikes_landed","clinch_strikes_attempts","ground_strikes_landed",
                    "ground_strikes_attempts"]
            fight_stats_old = pd.DataFrame(columns=fight_cols)
            fighter_cols = ["name","height","reach","stance","dob","url","SLpM","Str_Acc","SApM","Str_Def","TD_Avg",
                           "TD_Acc","TD_Def","Sub_Avg","num_fights"]
            fighter_stats_old = pd.DataFrame(columns=fighter_cols)

        # update fight history
        fight_stats_updated = self.update_fight_stats(fight_stats_old)

        ### For some reason these columns get put in as object dtypes rather than int64s when they're new stats
        fight_stats_updated = fight_stats_updated.astype({"knockdowns": np.int64, "sub_attempts": np.int64, "reversals": np.int64})

        # update fighter stats
        fighter_stats_updated = self.update_fighter_details(fight_stats_updated.fighter_url.unique(), fighter_stats_old)

        return fight_stats_updated, fighter_stats_updated

    def write_fights(self, fight_stats_updated, fighter_stats_updated, fight_stats, fighter_stats):
        """
        Write dataframe to csv
        """
        fight_stats_updated.to_csv(fight_stats, index=False)
        fighter_stats_updated.to_csv(fighter_stats, index=False)

        return True

    def update_fight_stats(self, old_stats):
        """
        Updates fight stats with newer fights
        takes dataframe of fight stats as input
        """
        http = self.create_retries()

        url = 'http://ufcstats.com/statistics/events/completed?page=all'
        page = http.get(url)

        soup = BeautifulSoup(page.content, "html.parser")

        events_table = soup.select_one('tbody')
        events = [event['href'] for event in events_table.select('a')[1:]]  # omit first event, future event

        saved_events = set(old_stats.event_url.unique())
        new_stats = pd.DataFrame()
        for event in events:
            if event in saved_events:
                break
            else:
                print(event)
                stats = self.get_fight_card(event)
                new_stats = pd.concat([new_stats, stats], axis=0)

        updated_stats = pd.concat([new_stats, old_stats], axis=0)
        updated_stats = updated_stats.reset_index(drop=True)

        return (updated_stats)

    def update_fighter_details(self, fighter_urls, saved_fighters):
        """
        Updates fighter attributes with new fighters not yet saved yet
        """
        fighter_details = {'name': [], 'height': [], 'reach': [], 'stance': [], 'dob': [], 'url': []}
        # fighter_urls = set(fighter_urls)
        saved_fighter_urls = set(saved_fighters.url.unique())

        for f_url in fighter_urls:
            if f_url in saved_fighter_urls:
                pass
            else:
                print('adding new fighter:', f_url)
                page = self.http.get(f_url)
                soup = BeautifulSoup(page.content, "html.parser")

                fighter_name = soup.find('span', class_='b-content__title-highlight').text.strip()
                fighter_details['name'].append(fighter_name)

                fighter_details['url'].append(f_url)

                # Stat box 1
                fighter_attr = soup.find('div',
                                         class_='b-list__info-box b-list__info-box_style_small-width js-guide').select(
                    'li')
                for i in range(len(fighter_attr)):
                    attr = fighter_attr[i].text.split(':')[-1].strip()
                    if i == 0:
                        fighter_details['height'].append(attr)
                    elif i == 1:
                        pass  # weight is always just whatever weightclass they were fighting at
                    elif i == 2:
                        fighter_details['reach'].append(attr)
                    elif i == 3:
                        fighter_details['stance'].append(attr)
                    else:
                        fighter_details['dob'].append(attr)

                # Stat box 2
                fighter_stats = soup.find('div',
                                          class_='b-list__info-box b-list__info-box_style_middle-width js-guide clearfix').select(
                    'li')
                for x in fighter_stats:
                    line = x.text.split(":")
                    if len(line) > 1:  # prevents the \n\n\n\n lines from being read
                        label = line[0].strip().replace('.', '').replace(' ', '_')
                        stat = line[1].strip().replace('%', '')
                        if label in fighter_details:
                            fighter_details[label].append(stat)
                        else:
                            fighter_details[label] = [stat]

                # Stat box 3
                label = "num_fights"
                ufc_fights = soup.find_all("a", href=re.compile(r"ufcstats\.com/event-details/"))
                num_fights = len(ufc_fights)
                if label in fighter_details:
                    fighter_details[label].append(num_fights)
                else:
                    fighter_details[label] = [num_fights]

        new_fighters = pd.DataFrame(fighter_details)
        updated_fighters = pd.concat([new_fighters, saved_fighters])
        updated_fighters = updated_fighters.reset_index(drop=True)

        return updated_fighters

    def get_fight_card(self, url):
        """
        Gets fight stats for all fights on a card
        """
        page = self.http.get(url)
        soup = BeautifulSoup(page.content, "html.parser")

        fight_card = pd.DataFrame()
        date = soup.select_one('li.b-list__box-list-item').text.strip().split('\n')[-1].strip()
        rows = soup.select('tr.b-fight-details__table-row')[1:]
        top5 = 1
        for row in rows:
            fight_det = {'date': [], 'fight_url': [], 'event_url': [], 'result': [], 'last_5_comps_per_event': [], 'fighter': [], 'opponent': [],
                         'division': [], 'method': [],
                         'round': [], 'time': [], 'fighter_url': [], 'opponent_url': []}
            fight_det['date'] += [date, date]

            if top5 <= 5:
                fight_det['last_5_comps_per_event'] += [1, 1]
                top5 += 1
            else:
                fight_det['last_5_comps_per_event'] += [0, 0]

            # add date of fight
            fight_det['event_url'] += [url, url]  # add event url
            cols = row.select('td')
            for i in range(len(cols)):
                if i in set([2, 3, 4, 5]):  # skip sub, td, pass, strikes
                    pass
                elif i == 0:  # get fight url and results
                    fight_url = cols[i].select_one('a')['href']  # get fight url
                    fight_det['fight_url'] += [fight_url, fight_url]

                    results = cols[i].select('p')
                    if len(results) == 2:  # was a draw, table shows two draws
                        fight_det['result'] += ['D', 'D']
                    else:  # first fighter won, second lost
                        fight_det['result'] += ['W', 'L']

                elif i == 1:  # get fighter names and fighter urls
                    fighter_1 = cols[i].select('p')[0].text.strip()
                    fighter_2 = cols[i].select('p')[1].text.strip()

                    fighter_1_url = cols[i].select('a')[0]['href']
                    fighter_2_url = cols[i].select('a')[1]['href']

                    fight_det['fighter'] += [fighter_1, fighter_2]
                    fight_det['opponent'] += [fighter_2, fighter_1]

                    fight_det['fighter_url'] += [fighter_1_url, fighter_2_url]
                    fight_det['opponent_url'] += [fighter_2_url, fighter_1_url]
                elif i == 6:  # get division
                    division = cols[i].select_one('p').text.strip()
                    fight_det['division'] += [division, division]
                elif i == 7:  # get method
                    method = cols[i].select_one('p').text.strip()
                    fight_det['method'] += [method, method]
                elif i == 8:  # get round
                    rd = cols[i].select_one('p').text.strip()
                    fight_det['round'] += [rd, rd]
                elif i == 9:  # get time
                    time = cols[i].select_one('p').text.strip()
                    fight_det['time'] += [time, time]

            fight_det = pd.DataFrame(fight_det)
            # get striking details
            str_det = self.get_fight_stats(fight_url)
            if str_det is None:
                pass
            else:
                # join to fight details
                fight_det = pd.merge(fight_det, str_det, on='fighter', how='left', copy=False)
                # add fight details to fight card
                fight_card = pd.concat([fight_card, fight_det], axis=0)
        fight_card = fight_card.reset_index(drop=True)
        return fight_card

    def get_fight_stats(self, url):
        """
        Gets individual fight stats
        """
        page = self.http.get(url)
        soup = BeautifulSoup(page.content, "html.parser")
        fd_columns = {'fighter': [], 'knockdowns': [], 'sig_strikes': [], 'total_strikes': [], 'takedowns': [],
                      'sub_attempts': [], 'reversals': [],
                      'control': []}

        # gets overall fight details
        fight_details = soup.select_one('tbody.b-fight-details__table-body')
        if fight_details == None:
            print('missing fight details for:', url)
            return None
        else:
            referee = soup.select_one('i.b-fight-details__text-item:nth-child(5) > span:nth-child(2)').text.strip()
            time_format = soup.select_one('i.b-fight-details__text-item:nth-child(4)').text.strip()
            if "(" in time_format: ## Is this outdated based on ufcstats update?
                time_format = time_format.split("(")[1][:-1]
            else:
                time_format = time_format.split()[-1]
                if time_format == "Limit":
                    time_format = "No Time Limit"

            fd_cols = fight_details.select('td.b-fight-details__table-col')
            for i in range(len(fd_cols)):
                # skip 3 and 6: strike % and takedown %, will calculate these later
                if i == 3 or i == 6:
                    pass
                else:
                    col = fd_cols[i].select('p')
                    for row in col:
                        data = row.text.strip()
                        if i == 0:  # add to fighter
                            fd_columns['fighter'].append(data)
                        elif i == 1:  # add to sig strikes
                            fd_columns['knockdowns'].append(data)
                        elif i == 2:  # add to total strikes
                            fd_columns['sig_strikes'].append(data)
                        elif i == 4:  # add to total strikes
                            fd_columns['total_strikes'].append(data)
                        elif i == 5:  # add to takedowns
                            fd_columns['takedowns'].append(data)
                        elif i == 7:  # add to sub attempts
                            fd_columns['sub_attempts'].append(data)
                        elif i == 8:  # add to reversals
                            fd_columns['reversals'].append(data)
                        elif i == 9:  # add to control
                            fd_columns['control'].append(data)
            ov_details = pd.DataFrame(fd_columns)

            ov_details.insert(1, 'time_format', time_format)
            ov_details.insert(1, 'referee', referee)

            # get sig strike details
            sig_strike_details = soup.find('p', class_='b-fight-details__collapse-link_tot',
                                           text=re.compile('Significant Strikes')).find_next('tbody',
                                                                                             class_='b-fight-details__table-body')
            sig_columns = {'fighter': [], 'head_strikes': [], 'body_strikes': [], 'leg_strikes': [], 'distance_strikes': [],
                           'clinch_strikes': [], 'ground_strikes': []}
            fd_cols = sig_strike_details.select('td.b-fight-details__table-col')
            for i in range(len(fd_cols)):
                # skip 1, 2 (sig strikes, sig %)
                if i == 1 or i == 2:
                    pass
                else:
                    col = fd_cols[i].select('p')
                    for row in col:
                        data = row.text.strip()
                        if i == 0:  # add to fighter
                            sig_columns['fighter'].append(data)
                        elif i == 3:  # add to head strikes
                            sig_columns['head_strikes'].append(data)
                        elif i == 4:  # add to body strikes
                            sig_columns['body_strikes'].append(data)
                        elif i == 5:  # add to leg strikes
                            sig_columns['leg_strikes'].append(data)
                        elif i == 6:  # add to distance strikes
                            sig_columns['distance_strikes'].append(data)
                        elif i == 7:  # add to clinch strikes
                            sig_columns['clinch_strikes'].append(data)
                        elif i == 8:  # add to ground strikes
                            sig_columns['ground_strikes'].append(data)
            sig_details = pd.DataFrame(sig_columns)

            cfd = pd.merge(ov_details, sig_details, on='fighter', how='left', copy=False)

            cfd['takedowns_landed'] = cfd.takedowns.str.split(' of ').str[0].astype(int)
            cfd['takedowns_attempts'] = cfd.takedowns.str.split(' of ').str[-1].astype(int)
            cfd['sig_strikes_landed'] = cfd.sig_strikes.str.split(' of ').str[0].astype(int)
            cfd['sig_strikes_attempts'] = cfd.sig_strikes.str.split(' of ').str[-1].astype(int)
            cfd['total_strikes_landed'] = cfd.total_strikes.str.split(' of ').str[0].astype(int)
            cfd['total_strikes_attempts'] = cfd.total_strikes.str.split(' of ').str[-1].astype(int)
            cfd['head_strikes_landed'] = cfd.head_strikes.str.split(' of ').str[0].astype(int)
            cfd['head_strikes_attempts'] = cfd.head_strikes.str.split(' of ').str[-1].astype(int)
            cfd['body_strikes_landed'] = cfd.body_strikes.str.split(' of ').str[0].astype(int)
            cfd['body_strikes_attempts'] = cfd.body_strikes.str.split(' of ').str[-1].astype(int)
            cfd['leg_strikes_landed'] = cfd.leg_strikes.str.split(' of ').str[0].astype(int)
            cfd['leg_strikes_attempts'] = cfd.leg_strikes.str.split(' of ').str[-1].astype(int)
            cfd['distance_strikes_landed'] = cfd.distance_strikes.str.split(' of ').str[0].astype(int)
            cfd['distance_strikes_attempts'] = cfd.distance_strikes.str.split(' of ').str[-1].astype(int)
            cfd['clinch_strikes_landed'] = cfd.clinch_strikes.str.split(' of ').str[0].astype(int)
            cfd['clinch_strikes_attempts'] = cfd.clinch_strikes.str.split(' of ').str[-1].astype(int)
            cfd['ground_strikes_landed'] = cfd.ground_strikes.str.split(' of ').str[0].astype(int)
            cfd['ground_strikes_attempts'] = cfd.ground_strikes.str.split(' of ').str[-1].astype(int)

            cfd = cfd.drop(['takedowns', 'sig_strikes', 'total_strikes', 'head_strikes', 'body_strikes', 'leg_strikes',
                            'distance_strikes',
                            'clinch_strikes', 'ground_strikes'], axis=1)

            return (cfd)

    def get_all_fight_stats(self):
        """
        Gets stats on all fights on all cards
        """
        url = 'http://ufcstats.com/statistics/events/completed?page=all'
        page = self.http.get(url)
        soup = BeautifulSoup(page.content, "html.parser")

        events_table = soup.select_one('tbody')
        events = [event['href'] for event in events_table.select('a')[1:]]  # omit first event, future event

        fight_stats = pd.DataFrame()
        for event in events:
            print(event)
            stats = self.get_fight_card(event)
            fight_stats = pd.concat([fight_stats, stats], axis=0)

        # Change location of referee and round times
        # cols = fight_stats.columns.tolist()
        # cols = cols[:8] + cols[16:18] + cols[8:16] + cols[18:]
        # fight_stats = fight_stats[cols]

        fight_stats = fight_stats.reset_index(drop=True)
        return fight_stats