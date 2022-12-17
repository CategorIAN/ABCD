import pandas as pd
import numpy as np

class People:
    def __init__(self):
        df = pd.read_csv("People.csv", index_col=0)
        self.df = df



