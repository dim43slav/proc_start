import proc_start as ps


i_procuuid = 'FED3AD2CE2AD11ED8B4EFBF4E008EEF5'
i_dataset = 'AKKFLIGHTS'
i_scenario = 'MAIN'
i_continue_error = 'X'

if __name__ == '__main__':
    start = ps.ProcStart(i_procuuid, i_scenario, i_dataset, i_continue_error)
    end = start.check_empty()
    if end != None:
        print(f'Error: e_result = {end}')