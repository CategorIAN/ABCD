from Form import Form

class General (Form):
    def __init__(self):
        name = "General"
        col_mapping = {
            "Timestamp": "Timestamp",
            # ===========================
            "Email Address": "Email",
            # ===========================
            "What is your name?": "Name",
            # ===========================
            "You are currently in my tabletop gaming group. What would you like your status to be? " \
            "(If you pick the second or third option, you may skip the rest of the questions in this survey.)": "Status",
            # ===========================
            "Every invite you receive for a game event brings you down the queue, making you less likely to be invited " \
            "to the next game event. Therefore, it is important I know what games you are interested in playing. " \
            "Which of my games are you interested in playing?": "Games",
            # ===========================
            "What types of games do you enjoy playing?": "Game_Types",
            # ===========================
            "What is the maximum number of hours you are willing to play a game in one sitting?": "Max_Hours",
            # ===========================
            "Would you be willing to a game commitment over multiple days?": "Commitment",
            # ===========================
            "Are there games that you own and know how to play that you would enjoy bringing the game for game events? " \
            "If so, which games would you enjoy bringing? (You would be responsible for bringing the game and explaining " \
            "the rules.)": "Guest_Games",
            # ===========================
            "Which of my signature meals would you be willing to eat at events?": 'Meals',
            # ===========================
            "What are your food allergies?": "Allergies",
            # ===========================
            "What food and/or drinks would you be willing to bring to a gaming event?": "Guest_Food",
            # ===========================
            "It is efficient to communicate using a group communication platform for invitations and coordination " \
            "of details for events. Which of these group communication platforms would you be willing to use?": "Platforms"
        }
        grid_col_mapping = {
            "What times are you possibly available to play games?": (
            "Availability", lambda row: row.partition(' to')[0])
        }
        set_features = {'Games', 'Game_Types', 'Meals', 'Allergies', 'Platforms'}
        keys = ["Email", "Name"]
        make_active = True
        multchoice_cols = ["Status"]
        multchoice_optset = [
            [
                '"I would like to be active in your group." (You will be invited to game events.)',
                '"I am not available to participate in games for this season." (You will not be invited to game ' \
                'events, but you will be asked to update this survey at least once a year.)',
                '"I would like to be taken off of this gaming list." (You can ask to be put back on this list ' \
                'whenever you want.)'
            ]
        ]
        multchoice_newoptset = [['Active', 'Not_Now', 'Please_Remove']]
        linscale_cols = ["Max_Hours"]
        longans_cols = ['Guest_Games', 'Guest_Food']
        checkbox_cols = ["Games", "Game_Types", "Meals", "Platforms", "Allergies", "Commitment"]
        checkbox_optset = [
            ['Catan', 'Chess', 'Codenames', 'War of the Ring'],  # Games
            ['Abstract', 'Customizable', 'Family', 'Party', 'Strategy', 'Thematic', 'Wargames'],  # Game_Types
            ['Chicken Cacciatore', 'Halibut in Lemon Wine Sauce', 'Hot Crab Dip', 'Peanut Butter Hummus',
             'Quinoa Lentil Berry Salad', 'Rosemary Pork and Mushrooms', 'Spaghetti and Classic Marinara Sauce',
             'Spinach and Artichoke Dip', 'Sweet Potato Casserole'],  # Meals
            ['Google Groups [Group Email] (groups.google.com)', 'Evite (evite.com)',
             'Google Chat (chat.google.com)', 'Slack (slack.com)', 'Discord (discord.com)'],  # Platforms
            ['Gluten', 'Dairy', 'Peanuts', 'Shellfish'],  # Allergies
            ["Yes, I would be willing to commit to playing a long game over multiple days.",
             "Yes, I would be willing to participate in a tournament that lasts for multiple days."],  # Commitment
        ]
        checkbox_newoptset = [None, None, None, None, None, ["Long Game", "Tournament"]]
        otherset = [False, False, False, False, True, False]
        checkboxgrid_cols = ["Availability"]
        checkboxgrid_coloptset = [['Friday', 'Saturday', 'Sunday']]
        checkboxgrid_rowoptset = [["11:00 AM", "12:00 PM", "1:00 PM", "2:00 PM", "3:00 PM", "4:00 PM", "5:00 PM", "6:00 PM"]]
        super().__init__(name, col_mapping, grid_col_mapping, set_features, keys,
                 make_active, multchoice_cols, multchoice_optset, multchoice_newoptset,
                 linscale_cols, longans_cols, checkbox_cols, checkbox_optset, checkbox_newoptset, otherset,
                 checkboxgrid_cols, checkboxgrid_coloptset, checkboxgrid_rowoptset)





