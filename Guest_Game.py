from Form import Form

class Guest_Game (Form):
    def __init__(self):
        name = "GuestGame"
        col_mapping = {
            "Timestamp": "Timestamp",
            # ===========================
            "Email Address": "Email",
            # ===========================
            "What is your name?": "Name",
            # ===========================
            "What is the name of the game you will be providing and leading?  (You will be responsible for "
            "bringing the game and explaining the rules.)": "Guest Game",
            # ===========================
            "What is the maximum number of people that you would like to play this game? (See the box of the game "
            "to see what the creators of the game recommend for the max.)": "Max Players",
            # ===========================
            "What is the minimum number of people that you think is usually needed to make the game interesting. "
            "(We need to collectively get this minimum number of people committed to coming to the event at least "
            "a week before the event.)": "Min Players",
            # ===========================
            "How many people do you plan on finding to commit to coming to the event? (You should notify me if "
            "you change this number.)": "Guest Invite Number"
        }
        grid_col_mapping = {
            "What times and dates are you available to lead the event? (Weekend #1 of the Month)": (
                "Weekend #1", lambda row: row.partition(' to')[0]),
            "What times and dates are you available to lead the event? (Weekend #2 of the Month)": (
                "Weekend #2", lambda row: row.partition(' to')[0]),
            "What times and dates are you available to lead the event? (Weekend #3 of the Month)": (
                "Weekend #3", lambda row: row.partition(' to')[0]),
            "What times and dates are you available to lead the event? (Weekend #4 of the Month)": (
                "Weekend #4", lambda row: row.partition(' to')[0])
        }
        set_features = set()
        keys = ["Email", "Name"]
        make_active = False
        multchoice_cols = []
        multchoice_optset = []
        multchoice_newoptset = []
        linscale_cols = ["Max Players", "Min Players", "Guest Invite Number"]
        text_cols = ["Guest Game"]
        checkbox_cols = []
        checkbox_optset = []
        checkbox_newoptset = []
        otherset = []
        checkboxgrid_cols = ["Weekend #{}".format(i) for i in range(1, 5)]
        checkboxgrid_coloptset = [["Friday", "Saturday", "Sunday"] for i in range(1, 5)]
        checkboxgrid_rowoptset = [["11:00 AM", "12:00 PM", "1:00 PM", "2:00 PM",
                                   "3:00 PM", "4:00 PM", "5:00 PM", "6:00 PM"] for i in range(1, 5)]
        mergeTuple = ("General", ["Email Address", "What is your name?"], "Email Address")
        super().__init__(name, col_mapping, grid_col_mapping, set_features, keys,
                         make_active, multchoice_cols, multchoice_optset, multchoice_newoptset,
                         linscale_cols, text_cols, checkbox_cols, checkbox_optset, checkbox_newoptset, otherset,
                         checkboxgrid_cols, checkboxgrid_coloptset, checkboxgrid_rowoptset, mergeTuple)
