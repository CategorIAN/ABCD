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
        super().__init__('ABCD')
        self.F = ABCD_Form()
        df = df.rename(self.F.column_name_transform, axis=1)
        df = df.fillna("")
        df = self.setDF(df, self.F.set_features)
        df = self.removeDuplicates(df)
        self.status(df)
        self.availability()
        self.games()
        self.game_types()
        self.guest_games()
        self.meals()
        self.allergies()
        self.guest_food()
        self.platforms()

    ### Multiple Choice Questions---------------------------------------------------------------------------------------
    def status(self, df):
        active = '"I would like to be active in your group." (You will be invited to game events.)'
        not_now = '"I am not available to participate in games for this season." (You will not be invited to game ' \
                  'events, but you will be asked to update this survey at least once a year.)'
        please_remove = '"I would like to be taken off of this gaming list." (You can ask to be put back on this list ' \
                        'whenever you want.)'
        d = {active: 'Active', not_now: "Not_Now", please_remove: 'Please_Remove'}
        df['Status'] = df['Status'].map(d)
        self.active = df[df['Status'] == 'Active'].reset_index(drop=True)
        cleaned_df = self.stringDF(df, self.F.set_features).sort_values(by=['Status', 'Name']).reset_index(drop=True)
        self.save(cleaned_df, "cleaned")


    ### Checkbox Questions-----------------------------------------------------------------------------------------------
    def games(self, append = True):
        x = pd.Series(self.F.games).map(lambda g: (g, self.active['Games'].map(lambda s: int(g in s))))
        self.g = pd.DataFrame(dict([('Name', self.active['Name'])] + list(x)))
        if append:
            total = dict([('Name', 'Total')] + [(col, self.g[col].sum()) for col in self.g.columns[1:]])
            self.g = pd.concat([self.g, pd.DataFrame(total, index=[self.g.shape[0]])])
        self.save(self.g, "games")

    def game_types(self, append = True):
        x = pd.Series(self.F.game_types).map(lambda gt: (gt, self.active['Game_Types'].map(lambda s: int(gt in s))))
        self.gt = pd.DataFrame(dict([('Name', self.active['Name'])] + list(x)))
        if append:
            total = dict([('Name', 'Total')] + [(col, self.gt[col].sum()) for col in self.gt.columns[1:]])
            self.gt = pd.concat([self.gt, pd.DataFrame(total, index=[self.gt.shape[0]])])
        self.save(self.gt, "game_types")

    def meals(self, append = True):
        x = pd.Series(self.F.meals).map(lambda m: (m, self.active['Meals'].map(lambda s: int(m in s))))
        self.m = pd.DataFrame(dict([('Name', self.active['Name'])] + list(x)))
        if append:
            total = dict([('Name', 'Total')] + [(col, self.m[col].sum()) for col in self.m.columns[1:]])
            self.m = pd.concat([self.m, pd.DataFrame(total, index=[self.m.shape[0]])])
        self.save(self.m, "meals")

    def platforms(self, append = True):
        x = pd.Series(self.F.platforms).map(lambda p: (p, self.active['Platforms'].map(lambda s: int(p in s))))
        self.p = pd.DataFrame(dict([('Name', self.active['Name'])] + list(x)))
        if append:
            total = dict([('Name', 'Total')] + [(col, self.p[col].sum()) for col in self.p.columns[1:]])
            self.p = pd.concat([self.p, pd.DataFrame(total, index=[self.p.shape[0]])])
        self.save(self.p, "platforms")

    ### Checkbox Grid Questions----------------------------------------------------------------------------------------
    def availability(self, append = True):
        x = pd.Series(product(self.F.days, self.F.hours)).map(lambda dh: (dh[0], dh[1].partition(' to')[0]))
        y = x.map(lambda dh: ("{} [{}]".format(dh[0], dh[1]), self.active[dh[1]].map(lambda s: int(dh[0] in s))))
        self.av = pd.DataFrame(dict([('Name', self.active['Name'])] + list(y)))
        if append:
            total = dict([("Name", "Total")] + [(col, self.av[col].sum()) for col in self.av.columns[1:]])
            self.av = pd.concat([self.av, pd.DataFrame(total, index=[self.av.shape[0]])])
        self.save(self.av, "availability")

    ### Long Answer Questions------------------------------------------------------------------------------------------
    def guest_games(self):
        gg = self.active[['Name', 'Guest_Games']]
        gg = gg.loc[gg['Guest_Games'].map(lambda s: len(s) > 0)].reset_index(drop=True)
        self.gg = pd.DataFrame(gg)
        self.save(self.gg, "guest_games")

    def guest_food(self):
        gf = self.active[['Name', 'Guest_Games']]
        gf = gf.loc[gf['Guest_Games'].map(lambda s: len(s) > 0)].reset_index(drop=True)
        self.gf = pd.DataFrame(gf)
        self.save(self.gf, "guest_food")

    ### Checkbox Questions with Other-----------------------------------------------------------------------------------
    def allergies(self, append = True):
        x = pd.Series(self.F.allergies).map(lambda a: (a, self.active['Allergies'].map(lambda s: int(a in s))))
        self.a = pd.DataFrame(dict([('Name', self.active['Name'])] + list(x)))
        if append:
            total = dict([('Name', 'Total')] + [(col, self.a[col].sum()) for col in self.a.columns[1:]])
            self.a = pd.concat([self.a, pd.DataFrame(total, index=[self.a.shape[0]])])
        self.save(self.a, "allergies")
        y = self.active['Allergies'].map(lambda aa: aa.difference(set(self.F.allergies)))
        self.ea = pd.DataFrame({'Name': self.active['Name'], 'Allergies': y})
        self.ea = self.ea.loc[self.ea['Allergies'].map(lambda s: len(s) > 0)].reset_index(drop=True)
        self.ea = self.stringDF(self.ea, {'Allergies'})
        self.save(self.ea, "extra_allergies")












