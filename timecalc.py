import math


class TimeCalcDiff:
    def __init__(self, timestamp1: int, timestamp2: int):
        self.timestamp1 = timestamp1 
        self.timestamp2 = timestamp2

    def calculation(self):
        #print(self.timestamp1, self.timestamp2)
        if self.timestamp1 < self.timestamp2:
            sign_factor = -1
            lf_timestamp1 = self.timestamp1
            lf_timestamp2 = self.timestamp2 
        else:
            sign_factor = 1   
            lf_timestamp1 = self.timestamp2
            lf_timestamp2 = self.timestamp1

        lf_date1 = int(str(lf_timestamp1)[:8])
        lf_time1 = int(str(lf_timestamp1)[8:17])
        lf_date2 = int(str(lf_timestamp2)[:8])
        lf_time2 = int(str(lf_timestamp2)[8:17])

        difference = ((lf_date1 - lf_date2)*86400 + (lf_time1 - lf_time2)) * sign_factor

        return difference