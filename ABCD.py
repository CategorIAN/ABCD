import pandas as pd
import numpy as np
from People import People
import os
from copy import copy


class ABCD:

    def __init__(self, df):
        df = df.rename(self.rename_columns, axis=1)
        df = df.fillna("")
        df = self.setDF(df)
        df = self.removeDuplicates(df)
        df = df.drop(['Timestamp'], axis=1)
        df = df.sort_values(by='Name')
        df.reset_index(drop=True, inplace=True)
        df = self.status(df)
        self.active = df[df['Status'] == 'Active'].reset_index(drop=True)
        self.availability()
        self.games()
        self.game_types()
        self.guest_games()
        self.countMeals()
        self.countAllergies()
        self.df = self.stringDF(df)


    def status(self, df):
        active = '"I would like to be active in your group." (You will be invited to game events.)'
        not_now = '"I am not available to participate in games for this season." (You will not be invited to game ' \
                  'events, but you will be asked to update this survey at least once a year.)'
        please_remove = '"I would like to be taken off of this gaming list." (You can ask to be put back on this list ' \
                        'whenever you want.)'
        d = {active: 'Active', not_now: "Not_Now", please_remove: 'Please_Remove'}
        df['Status'] = df['Status'].apply(lambda x: d[x])
        return df

    def availability(self):
        av = {}
        av['Name'] = self.active['Name']
        total = {'Name': 'Total'}
        for day in ['Friday', 'Saturday', 'Sunday']:
            for h in self.hours:
                h = h.partition(' to')[0]
                t = 0
                a = []
                for i in range(self.active.shape[0]):
                        b = int(day in self.active.at[i, h])
                        a.append(b)
                        t += b
                av["{} [{}]".format(day, h)] = a
                total["{} [{}]".format(day, h)] = t
        self.av = pd.DataFrame(av).append(total, ignore_index = True)

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
        self.g = pd.DataFrame(g).append(total, ignore_index=True)

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
        self.gt = pd.DataFrame(gt).append(total, ignore_index=True)

    def guest_games(self):
        gg = {}
        gg['Name'] = self.active['Name']
        prompt = 'Are there games that you own and know how to play that you would enjoy bringing the game for game " \
                 "events? If so, which games would you enjoy bringing? (You would be responsible for bringing the game " \
                 "and explaining the rules.)'
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
        for meal in self.meals:
            t = 0
            a = []
            for i in range(self.active.shape[0]):
                b = int(meal in self.active.at[i, 'Meals'])
                a.append(b)
                t += b
            meals[meal] = a
            total[meal] = t
        self.gt = pd.DataFrame(meals).append(total, ignore_index=True)
        self.gt.to_csv(self.directory + "ABCD_meals.csv")

    def countAllergies(self):
        self.allergy_df = pd.DataFrame(index = self.active.index, columns=self.allergies)
        self.allergy_df.insert(0, 'Name', self.active['Name'])
        for i in range(self.active.shape[0]):
            their_allergies = copy(self.active.at[i, 'Allergies'])
            print(their_allergies)
            print(pd.Series(self.allergies).map(lambda allergy: int(allergy in their_allergies)))
            self.allergy_df.loc[i, self.allergies] = pd.Series(self.allergies).map(lambda allergy: int(allergy in their_allergies)).values
        self.allergy_df.to_csv(self.directory + "ABCD_allergies.csv")

    def setDF(self, df) -> pd.DataFrame:
        def toSet(string):
            s = set()
            for e in string.split(","):
                if len(e) > 0 and e != "set()":
                    s.add(e.strip(" {}'"))
            return s
        for column in {'Games', 'Game_Types', 'Meals', 'Allergies', 'Platform'}:
            df[column] = df[column].apply(lambda x: toSet(x))
        return df

    def stringDF(self, df) -> pd.DataFrame:
        def toString(ss):
            string = ""
            for s in ss:
                string = string + s + ", "
            return string.strip(", ")
        for column in {'Games', 'Game_Types', 'Meals', 'Allergies', 'Platform'}:
                df[column] = df[column].apply(toString)
        return df

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
    name = 'What is your name?'
    status_q = 'You are currently in my tabletop gaming group. What would you like your status to be? ' \
             '(If you pick the second or third option, you may skip the rest of the questions in this survey.)'

    games_q = 'Every invite you receive for a game event brings you down the queue, making you less likely to be invited ' \
            'to the next game event. Therefore, it is important I know what games you are interested in playing. ' \
            'Which of my games are you interested in playing?'
    game_types_q = 'What types of games do you enjoy playing?'
    max_hours = 'What is the maximum number of hours you are willing to play a game in one sitting?'
    commit = 'Would you be willing to a game commitment over multiple days?'
    own = 'Are there games that you own and know how to play that you would enjoy bringing the game for game events? ' \
          'If so, which games would you enjoy bringing? (You would be responsible for bringing the game and explaining ' \
          'the rules.)'

    meals_q = 'Which of my signature meals would you be willing to eat at events?'
    allergies = 'What are your food allergies?'
    bring_food = 'What food and/or drinks would you be willing to bring to a gaming event?'

    platform = 'It is efficient to communicate using a group communication platform for invitations and coordination ' \
               'of details for events. Which of these group communication platforms would you be willing to use?'

    rename_columns = {time_stamp: 'Timestamp', email: 'Email', name: 'Name', status_q: 'Status', games_q: 'Games',
                      game_types_q: 'Game_Types', max_hours: 'Max_Hours', commit: 'Commit', own: 'Own', meals_q: 'Meals',
                      allergies: 'Allergies', bring_food: 'Bring_Food', platform: 'Platform'}

    hours = ["11:00 AM to 12:00 PM", "12:00 PM to 1:00 PM", "1:00 PM to 2:00 PM", "2:00 PM to 3:00 PM",
             "3:00 PM to 4:00 PM", "4:00 PM to 5:00 PM", "5:00 PM to 6:00 PM", "6:00 PM to 7:00 PM"]
    question = "What times are you possibly available to play games?"

    meals = ['Chicken Cacciatore', 'Halibut in Lemon Wine Sauce', 'Hot Crab Dip', 'Peanut Butter Hummus',
            'Quinoa Lentil Berry Salad', 'Rosemary Pork and Mushrooms', 'Spaghetti and Classic Marinara Sauce',
             'Spinach and Artichoke Dip', 'Sweet Potato Casserole']

    allergies = ['Gluten', 'Dairy', 'Peanuts', 'Shellfish']

    for i in range(len(hours)):
        rename_columns["{} [{}]".format(question, hours[i])] = hours[i].partition(' to')[0]

    directory = os.getcwd() + '\\' + 'ABCD' + '\\'




