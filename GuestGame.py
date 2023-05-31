import pandas as pd
import numpy as np
from People import People
import os


class GuestGame:
    def __init__(self, df):
        df = df.rename(self.rename_columns, axis=1)
        df = df.fillna("")
        df = self.removeDuplicates(df)
        df = df.drop(['Timestamp'], axis=1)
        people = pd.read_csv(os.getcwd() + '\\' + 'Raw Data' + '\\' + "People.csv", index_col=0)
        df = people.merge(right = df, how = 'inner', on = 'Email')
        df = df.sort_values(by='Name')
        df.reset_index(drop=True, inplace=True)
        self.df = df
        self.df.to_csv(self.directory + "GG_cleaned.csv")

    def availability(self, name, month):
        i = self.df.loc[lambda df: df["Name"] == name].index[0]
        (game, min, max, get) = tuple(self.df.loc[i, ["Guest Game", "Min Players", "Max Players", "Guest Invite Number"]])
        for wk in range(1, 5):
            av = {}
            for day in ['Friday', 'Saturday', 'Sunday']:
                a = []
                for h in self.hours:
                    wk_h = "{} (Weekend #{})".format(h, wk)
                    b = int(day in self.df.at[i, wk_h])
                    a.append(b)
                av[day] = a
            pd.DataFrame(av, index=self.hours).to_csv(self.directory + name + '\\' + month + '\\'
                          "{} On Weekend #{} (Min = {}, Max = {}, GuestGet = {}).csv".format(game, wk, min, max, get))

    def removeDuplicates(self, df):
        emails = set()
        for i in reversed(range(df.shape[0])):
            e = df.at[i, 'Email']
            if e in emails:
                df = df.drop(i)
            else:
                emails.add(e)
        return df

    time_stamp = 'Timestamp'
    email = 'Email Address'

    guest_game_q = 'What is the name of the game you will be providing and leading?  (You will be responsible for ' \
                   'bringing the game and explaining the rules.)'

    max_players = 'What is the maximum number of people that you would like to play this game? (See the box of the game ' \
                  'to see what the creators of the game recommend for the max.)'

    min_players = 'What is the minimum number of people that you think is usually needed to make the game interesting. ' \
                  '(We need to collectively get this minimum number of people committed to coming to the event at least ' \
                  'a week before the event.)'

    guest_invite_number = "How many people do you plan on finding to commit to coming to the event? (You should notify me if " \
                   "you change this number.)"


    rename_columns = {time_stamp: 'Timestamp', email: 'Email', guest_game_q: 'Guest Game', max_players: 'Max Players',
                      min_players: 'Min Players', guest_invite_number: 'Guest Invite Number'}

    form_hours = ["11:00 AM to 12:00 PM", "12:00 PM to 1:00 PM", "1:00 PM to 2:00 PM", "2:00 PM to 3:00 PM",
             "3:00 PM to 4:00 PM", "4:00 PM to 5:00 PM", "5:00 PM to 6:00 PM", "6:00 PM to 7:00 PM"]

    hours = pd.Series(form_hours).map(lambda h: h.partition(' to')[0])

    avail_q = "What times and dates are you available to lead the event?"

    for i in range(1, 5):
        for j in range(len(form_hours)):
            prompt = "{} (Weekend #{} of the Month) [{}]".format(avail_q, i, form_hours[j])
            rename_columns[prompt] = "{} (Weekend #{})".format(hours[j], i)

    directory = os.getcwd() + '\\' + 'Guest Games' + '\\'




