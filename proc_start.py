import pandas as pd
from sqlalchemy import create_engine, inspect
import connection as cn
import timecalc as tc
import math
import datetime

#разобраться с статусами выполненных запросов (может быть через try catch)
class ProcStart:
    def __init__(self, procuuid: str, scenario: str, dataset: str, continue_error):
        self.procuuid = procuuid
        self.scenario = scenario
        self.dataset = dataset
        self.c_max_wait_time_sec = 7200
        self.i_continue_error = continue_error

    def check_empty(self):
        if self.procuuid == '':
            return 11
        elif self.dataset == '':
            return 12

        query = cn.Connection()
        wa_dataset = query.query(f'''SELECT *
                                    FROM ddogadkin.zwt_dataset1
                                    WHERE
                                        procuuid = \'{self.procuuid}\' AND
                                        scenario = \'{self.scenario}\'
                                    LIMIT 1''', 'S')

        #Если совпадает uuid и scenario, то ошибка

        if len(wa_dataset) > 0:
            return 21

        query = cn.Connection()
        wa_dataset_dp = query.query(f'''SELECT *
                                    FROM ddogadkin.zwt_dataset1_dp
                                    WHERE
                                        dataset = \'{self.dataset}\' AND
                                        scenario = \'{self.scenario}\'
                                    LIMIT 1''', 'S')

        #если отсутствует датасет и сценарий, то ошибка

        if len(wa_dataset_dp) == 0:
            return 22

        wa_dataset_dp_wait_time = wa_dataset_dp[0][3]
        if wa_dataset_dp_wait_time <= 0:
                wa_dataset_dp_wait_time = self.c_max_wait_time_sec

        wa_dataset_dp_dataset = wa_dataset_dp[0][0]
        wa_dataset_dataset = wa_dataset_dp_dataset

        wa_dataset_dp_scenario = wa_dataset_dp[0][1]
        wa_dataset_scenario = wa_dataset_dp_scenario

        query = cn.Connection()
        wa_dataset = query.query(f'''SELECT *
                                    FROM ddogadkin.zwt_dataset1
                                    WHERE
                                        dataset = \'{wa_dataset_dp_dataset}\'
                                        AND scenario = \'{wa_dataset_dp_scenario}\'
                                    LIMIT 1 FOR UPDATE''', 'S')
        #su-sybrc 8??

        wa_dataset_procstatus = wa_dataset[0][3]
        wa_dataset_timestamp = wa_dataset[0][4]
        l_timestamp_1st_short = math.floor(wa_dataset_timestamp/1000)

        l_timestampl = datetime.datetime.now()
        l_timestampl = l_timestampl.strftime('%Y%m%d%H%M%S%f')
        wa_dataset_timestamp = l_timestampl[:17]
        l_timestampl_short = l_timestampl[:14]

        if wa_dataset_procstatus == '' or wa_dataset_procstatus == 'C':
            pass
        elif wa_dataset_procstatus == 'P':
            timecalc = tc.TimeCalcDiff(int(l_timestamp_1st_short), int(l_timestampl_short))
            difference = timecalc.calculation()
            if difference <= wa_dataset_dp_wait_time or self.i_continue_error == '':
                return 31
            wa_dataset_procstatus = 'E'
        elif wa_dataset_procstatus == 'E':
            if self.i_continue_error == '':
                return 32
        else:
            return 33

        query = cn.Connection()
        wa_dataset_ts = query.query(f'''SELECT *
                                    FROM ddogadkin.zwt_dataset1_ts
                                    WHERE
                                        dataset = \'{wa_dataset_dataset}\' AND
                                        scenario = \'{wa_dataset_scenario}\'''', 'S')

        if wa_dataset_procstatus == 'E':
            wa_dataset_ts_timestamp_old = wa_dataset_ts[0][4]
            wa_dataset_ts_timestamp = wa_dataset_ts_timestamp_old
        else:
            wa_dataset_ts_timestamp = wa_dataset_ts[0][3]
            wa_dataset_ts_timestamp_old = wa_dataset_ts_timestamp

        wa_dataset_procuuid = self.procuuid
        wa_dataset_procstatus = 'P'

        query = cn.Connection()
        query.query(f'''UPDATE ddogadkin.zwt_dataset1
                            SET procuuid = \'{wa_dataset_procuuid}\'
                                ,procstatus = \'{wa_dataset_procstatus}\'
                                ,"TIMESTAMP" = \'{wa_dataset_timestamp}\'
                            WHERE dataset = \'{self.dataset}\'
                                AND scenario = \'{self.scenario}\'''', 'U')


        query = cn.Connection()
        query.query(f'''UPDATE ddogadkin.zwt_dataset1_ts
                            SET "timestamp" = \'{wa_dataset_ts_timestamp}\'
                                ,timestamp_old = \'{wa_dataset_ts_timestamp_old}\'
                            WHERE dataset = \'{self.dataset}\'
                                AND scenario = \'{self.scenario}\'''', 'U')