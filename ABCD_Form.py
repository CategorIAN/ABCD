

class ABCD_Form:
    def __init__(self):
        self.hours = ["11:00 AM to 12:00 PM", "12:00 PM to 1:00 PM", "1:00 PM to 2:00 PM", "2:00 PM to 3:00 PM",
             "3:00 PM to 4:00 PM", "4:00 PM to 5:00 PM", "5:00 PM to 6:00 PM", "6:00 PM to 7:00 PM"]
        self.days = ['Friday', 'Saturday', 'Sunday']
        self.meals = ['Chicken Cacciatore', 'Halibut in Lemon Wine Sauce', 'Hot Crab Dip', 'Peanut Butter Hummus',
                 'Quinoa Lentil Berry Salad', 'Rosemary Pork and Mushrooms', 'Spaghetti and Classic Marinara Sauce',
                 'Spinach and Artichoke Dip', 'Sweet Potato Casserole']
        self.allergies = ['Gluten', 'Dairy', 'Peanuts', 'Shellfish']
        self.platforms = ['Google Groups [Group Email] (groups.google.com)',
                     'Evite (evite.com)',
                     'Google Chat (chat.google.com)',
                     'Slack (slack.com)',
                     'Discord (discord.com)']
        self.set_features = {'Games', 'Game_Types', 'Meals', 'Allergies', 'Platforms'}

    def column_name_transform(self, column):
        if column == "Email Address":
            return 'Email'
        if column == "What is your name?":
            return 'Name'
        if column == 'You are currently in my tabletop gaming group. What would you like your status to be? ' \
             '(If you pick the second or third option, you may skip the rest of the questions in this survey.)':
            return 'Status'
        if column == 'Every invite you receive for a game event brings you down the queue, making you less likely to be invited ' \
            'to the next game event. Therefore, it is important I know what games you are interested in playing. ' \
            'Which of my games are you interested in playing?':
            return 'Games'
        if column == 'What types of games do you enjoy playing?':
            return 'Game_Types'
        if column == 'What is the maximum number of hours you are willing to play a game in one sitting?':
            return 'Max_Hours'
        if column == 'Would you be willing to a game commitment over multiple days?':
            return 'Commit'
        if column == 'Are there games that you own and know how to play that you would enjoy bringing the game for game events? ' \
          'If so, which games would you enjoy bringing? (You would be responsible for bringing the game and explaining ' \
          'the rules.)':
            return 'Own'
        if column == 'Which of my signature meals would you be willing to eat at events?':
            return 'Meals'
        if column == 'What are your food allergies?':
            return 'Allergies'
        if column == 'What food and/or drinks would you be willing to bring to a gaming event?':
            return 'Guest_Food'
        if column == 'It is efficient to communicate using a group communication platform for invitations and coordination ' \
               'of details for events. Which of these group communication platforms would you be willing to use?':
            return 'Platforms'

        row_function = lambda row: row.partition(' to')[0]

        return self.grid_dict("What times are you possibly available to play games?", self.hours, row_function)(column)

    def grid_dict(self, question, rows, row_function):
        d = dict([("{} [{}]".format(question, row), row_function(row)) for row in rows])
        def f(key):
            if key in d:
                return d[key]
            else:
                return key
        return f
