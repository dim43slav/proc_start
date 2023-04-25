import proc_start as ps


i_procuuid = '321321321312321321337'
i_dataset = 'AKKFLIGHTS'
i_scenario = 'MAIN'
i_continue_error = 'X'

if __name__ == '__main__':
    start = ps.ProcStart(i_procuuid, i_scenario, i_dataset, i_continue_error)
    fin = start.check_empty()
    print(fin)