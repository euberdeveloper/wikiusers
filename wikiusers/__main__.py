from wikiusers.main import run
from wikiusers.metrics import MonthlyDropoff, MonthlyTotalPopulation, MonthlyActivePopulation, MonthlyDropoffOverActivePopulation

if __name__ == '__main__':
    run()


# shit = MonthlyDropoff(lang='ca')
# shit.compute()
# shit.save_json()

# shit = MonthlyTotalPopulation(lang='ca')
# shit.compute()
# shit.save_json()

# shit = MonthlyActivePopulation(lang='ca', active_per_month_thr=1000)
# shit.compute()
# shit.save_json()

# shit = MonthlyDropoffOverActivePopulation(lang='ca', active_per_month_thr=1000)
# shit.compute()
# shit.save_json()
# MonthlyDropoffOverActivePopulation.show_graph([
#     (12, 5),
#     (12, 15),
#     (12, 50),
#     (12, 200),
#     (12, 1000)
# ], 'ca')