import pandas as pd
import numpy as np
from People import People
import os
from copy import copy
from ABCD_Form import ABCD_Form
from Form import Form
from itertools import product


class ABCD (Form):
    def __init__(self, df):
        self.F = ABCD_Form()
        self.directory = os.getcwd() + '\\' + 'ABCD' + '\\'
        df = df.rename(self.F.column_name_transform, axis=1)
        df = df.fillna("")
        df = self.setDF(df, self.F.set_features)
        df = self.removeDuplicates(df)
        df = self.status(df)
        self.active = df[df['Status'] == 'Active'].reset_index(drop=True)
        self.availability()
        self.games()
        self.game_types()
        self.guest_games()
        self.countMeals()
        self.countAllergies()
        self.guest_food()
        self.countPlatforms()
        self.df = self.stringDF(df, self.F.set_features)


    def status(self, df):
        active = '"I would like to be active in your group." (You will be invited to game events.)'
        not_now = '"I am not available to participate in games for this season." (You will not be invited to game ' \
                  'events, but you will be asked to update this survey at least once a year.)'
        please_remove = '"I would like to be taken off of this gaming list." (You can ask to be put back on this list ' \
                        'whenever you want.)'
        d = {active: 'Active', not_now: "Not_Now", please_remove: 'Please_Remove'}
        df['Status'] = df['Status'].map(d)
        return df

    def availability(self, append = True):
        x = pd.Series(product(self.F.days, self.F.hours)).map(lambda dh: (dh[0], dh[1].partition(' to')[0]))
        y = x.map(lambda dh: ("{} [{}]".format(dh[0], dh[1]), self.active[dh[1]].map(lambda s: int(dh[0] in s))))
        self.av = pd.DataFrame(dict([('Name', self.active['Name'])] + list(y)))
        if append:
            total = dict([("Name", "Total")] + [(col, self.av[col].sum()) for col in self.av.columns])
            self.av = pd.concat([self.av, pd.DataFrame(total, index=[self.active.shape[0]])])

    def games(self):
        g = {}
        g['Name'] = self.active['Name']
        total = {'Name': 'Total'}
        for game in ['Catan', 'Chess', 'Codenames', 'War of the Ring']:
            t = 0
            a = []
            for i in range(self.active.shape[0]):
                b = int(game in self.active.at[i, 'Games'])
                a.append(b)
                t += b
            g[game] = a
            total[game] = t
        self.g = pd.concat([pd.DataFrame(g), pd.DataFrame(total, index=[self.active.shape[0]])])

    def game_types(self):
        gt = {}
        gt['Name'] = self.active['Name']
        total = {'Name': 'Total'}
        for type in ['Abstract', 'Customizable', 'Family', 'Party', 'Strategy',
                     'Thematic', 'Wargames']:
            t = 0
            a = []
            for i in range(self.active.shape[0]):
                b = int(type in self.active.at[i, 'Game_Types'])
                a.append(b)
                t += b
            gt[type] = a
            total[type] = t
        self.gt = pd.concat([pd.DataFrame(gt), pd.DataFrame(total, index=[self.active.shape[0]])])

    def guest_games(self):
        gg = {}
        gg['Name'] = self.active['Name']
        gg['Guest Games'] = self.active['Own']
        gg = pd.DataFrame(gg)
        index = []
        for i in range(gg.shape[0]):
            if len(gg.at[i, 'Guest Games']) > 0:
                index.append(i)
        gg = gg.filter(items = index, axis=0).reset_index(drop=True)
        self.gg = pd.DataFrame(gg)

    def countMeals(self):
        meals = {}
        meals['Name'] = self.active['Name']
        total = {'Name': 'Total'}
        for meal in self.F.meals:
            t = 0
            a = []
            for i in range(self.active.shape[0]):
                b = int(meal in self.active.at[i, 'Meals'])
                a.append(b)
                t += b
            meals[meal] = a
            total[meal] = t
        self.meals_df = pd.concat([pd.DataFrame(meals), pd.DataFrame(total, index=[self.active.shape[0]])])
        self.meals_df.to_csv(self.directory + "ABCD_meals.csv")



    def countAllergies(self):
        self.allergy_df = pd.DataFrame(index = self.active.index, columns=self.F.allergies)
        extra_allergies = {}
        self.allergy_df.insert(0, 'Name', self.active['Name'])
        for i in range(self.active.shape[0]):
            their_allergies = copy(self.active.at[i, 'Allergies'])
            self.allergy_df.loc[i, self.F.allergies] = pd.Series(self.F.allergies).map(lambda allergy: int(allergy in their_allergies)).values
            other = their_allergies.difference(set(self.F.allergies))
            if len(other) > 0: extra_allergies[self.active.at[i, 'Name']] = other
        self.allergy_df.to_csv(self.directory + "ABCD_allergies.csv")
        self.extra_allergies = pd.DataFrame(pd.Series(data = extra_allergies, name = 'Allergies'))
        self.extra_allergies = self.stringDF(self.extra_allergies, {'Allergies'}).reset_index(names=['Name'])
        self.extra_allergies.to_csv(self.directory + "ABCD_extra_allergies.csv")

    def guest_food(self):
        gf = {}
        gf['Name'] = self.active['Name']
        gf['Guest Food'] = self.active['Guest_Food']
        gf = pd.DataFrame(gf)
        index = []
        for i in range(gf.shape[0]):
            if len(gf.at[i, 'Guest Food']) > 0:
                index.append(i)
        gf = gf.filter(items = index, axis=0).reset_index(drop=True)
        self.gf = pd.DataFrame(gf)
        self.gf.to_csv(self.directory + "ABCD_guestfood.csv")

    def countPlatforms(self):
        platforms = {}
        platforms['Name'] = self.active['Name']
        total = {'Name': 'Total'}
        for platform in self.F.platforms:
            t = 0
            a = []
            for i in range(self.active.shape[0]):
                b = int(platform in self.active.at[i, 'Platforms'])
                a.append(b)
                t += b
            platforms[platform] = a
            total[platform] = t
        self.F.platforms = pd.concat([pd.DataFrame(platforms), pd.DataFrame(total, index=[self.active.shape[0]])])
        self.F.platforms.to_csv(self.directory + "ABCD_platforms.csv")








