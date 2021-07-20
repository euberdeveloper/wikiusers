from wikiusers.main import run
from wikiusers.metrics import MonthlyDropoff, MonthlyTotalPopulation, MonthlyActivePopulation, MonthlyDropoffOverActivePopulation, AdminsHistory

# if __name__ == '__main__':
#     run()


# shit = MonthlyDropoff(lang='ca')
# shit.compute()
# shit.save_json()

# shit = MonthlyTotalPopulation(lang='ca')
# shit.compute()
# shit.save_json()

# shit = MonthlyActivePopulation(lang='ca', active_per_month_thr=1000)
# shit.compute()
# shit.save_json()

# shit = MonthlyDropoffOverActivePopulation(lang='es', active_per_month_thr=200)
# shit.compute()
# shit.save_json()
MonthlyDropoffOverActivePopulation.show_graph([
    (12, 5, 'blue'),
    (12, 15, 'red'),
    (12, 50, 'green'),
    (12, 200, 'orange'),
    (12, 1000, 'purple')
], 'it')

# shit = AdminsHistory(lang='ca')
# shit.compute()
# shit.save_json()
# AdminsHistory.show_graphs('ca')