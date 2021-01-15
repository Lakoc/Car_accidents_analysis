import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

car_types = {1: "ALFA-ROMEO", 2: "AUDI", 3: "AVIA", 4: "BMW", 5: "CHEVROLET", 6: "CHRYSLER", 7: "CITROEN",
             8: "DACIA", 9: "DAEWOO", 10: "DAF", 11: "DODGE", 12: "FIAT", 13: "FORD", 14: "GAZ, VOLHA",
             15: "FERRARI", 16: "HONDA", 17: "HYUNDAI", 18: "IFA", 19: "IVECO", 20: "JAGUAR", 21: "JEEP",
             22: "LANCIA", 23: "LAND ROVER", 24: "LIAZ", 25: "MAZDA", 26: "MERCEDES", 27: "MITSUBISHI",
             28: "MOSKVIČ", 29: "NISSAN", 30: "OLTCIT", 31: "OPEL", 32: "PEUGEOT", 33: "PORSCHE", 34: "PRAGA",
             35: "RENAULT", 36: "ROVER", 37: "SAAB", 38: "SEAT", 39: "ŠKODA", 40: "SCANIA", 41: "SUBARU",
             42: "SUZUKI", 43: "TATRA", 44: "TOYOTA", 45: "TRABANT", 46: "VAZ", 47: "VOLKSWAGEN", 48: "VOLVO",
             49: "WARTBURG", 50: "ZASTAVA", 51: "AGM", 52: "ARO", 53: "AUSTIN", 54: "BARKAS", 55: "DAIHATSU",
             56: "DATSUN", 57: "DESTACAR", 58: "ISUZU", 59: "KAROSA", 60: "KIA", 61: "LUBLIN", 62: "MAN",
             63: "MASERATI", 64: "MULTICAR", 65: "PONTIAC", 66: "ROSS", 67: "SIMCA", 68: "SSANGYONG", 69: "TALBOT",
             70: "TAZ", 71: "ZAZ", 72: "BOVA", 73: "IKARUS", 74: "NEOPLAN", 75: "OASA", 76: "RAF", 77: "SETRA",
             78: "SOR", 79: "APRILIA", 80: "CAGIVA", 81: "ČZ", 82: "DERBI", 83: "DUCATI", 84: "GILERA",
             85: "HARLEY", 86: "HERO", 87: "HUSQVARNA", 88: "JAWA", 89: "KAWASAKI", 90: "KTM", 91: "MALAGUTI",
             92: "MANET", 93: "MZ", 94: "PIAGGIO", 95: "SIMSON", 96: "VELOREX", 97: "YAMAHA",
             98: "jiné vyrobené v ČR", 99: "jiné vyrobené mimo ČR", 00: "žádná z uvedených"}


def save_show_fig(fig_location: str, show_figure: bool, fig: plt.Figure):
    """Show and save file according to provided params"""
    if fig_location:
        fig.savefig(fig_location)
    if show_figure:
        fig.show()


def get_dataset(filename: str) -> pd.DataFrame:
    """Get personal car accidents with filled car brand cleaned from accidents with alcohol or drugs"""
    df = pd.read_pickle(filename)
    print(f'Number of accidents: {df.shape[0]}')
    return df[((df['p11'] == 0) | (df['p11'] == 2)) & ((df['p44'] == 3) | (df['p44'] == 4))].dropna(subset=['p45a'])


def plot_car_type(df: pd.DataFrame, fig_location: str = None,
                  show_figure: bool = False):
    """Plot bar graph of accidents brand / hurt rate"""
    # count every hurt person count in each accident
    df['hurt'] = df['p13a'] + df['p13b'] + df['p13c']

    # group by brand of car and count hurt people and accidents count for each group
    df_grouped = df.groupby(['p45a']).aggregate({'p1': 'count', 'hurt': 'sum'}).rename(columns={'p1': 'count'})

    # remove values that are not known brands
    df_grouped.drop([98, 99, ], inplace=True)

    # remove brands that were filled wrong -> motorcycles, trucks or brands that have small number of samples
    df_clean = df_grouped.copy()
    df_clean = df_clean[df_clean['count'] > 500]

    # count hurt rate and add text brand for visualisation
    df_clean['hurt_rate'] = df_clean['hurt'] / df_clean['count']
    df_clean['brand'] = df_clean.index.map(car_types)

    # sort dataset
    df_clean = df_clean.sort_values(by=['hurt_rate'])

    # plot values
    sns.set_style("darkgrid")
    fig, ax = plt.subplots(1, 1, figsize=(8, 5))
    sns.barplot(x="hurt_rate", y="brand",
                data=df_clean, ax=ax, orient='h', palette="Reds_d")

    # set labels
    ax.set_xlabel("Podíl nehod se zraněním")
    ax.set_ylabel("Značka automobilu")
    ax.set_title('Výskyt zranění u nehod příslušných výrobců automobilů', fontsize=14, fontweight="bold")

    fig.tight_layout()
    save_show_fig(fig_location, show_figure, fig)


def create_table_by_year(df: pd.DataFrame):
    """Create latex table with overview of accidents with injury and year of production of car"""

    # remove unknown years
    df = df[df['p47'] != -1]
    df_clean = df.copy()

    # split years to bin
    df_clean['p47'] = df_clean['p47'].apply(lambda x: x + 2000 if x <= 20 else x + 1900)
    df_clean['p_year'] = pd.cut(df_clean['p47'], [1995, 2000, 2005, 2010, 2015, 2020],
                                labels=['95-00', '00-05', '05-10', '10-15', '15-20'])

    find_worst_scenario(df_clean)
    print_stats(df_clean)
    # group and aggregate interesting values
    df_grouped = df_clean.groupby(['p_year']).aggregate(
        {'p1': 'count', 'p13a': 'mean', 'p13b': 'mean', 'p13c': 'mean'})
    df_grouped[['p13a', 'p13b', 'p13c']] = df_grouped[['p13a', 'p13b', 'p13c']].multiply(100)
    # rename columns
    df_grouped = df_grouped.rename(
        columns={'p_year': 'rok výroby', 'p1': 'počet nehod', 'p13a': 'smrt [%]', 'p13b': 'těžké zranění [%]',
                 'p13c': 'lehké zranění [%]'})

    # create latex table and print it to stdout
    print(df_grouped.to_latex(float_format="%.1f"))


def find_worst_scenario(df: pd.DataFrame):
    """Find cars u don't want to sit in :)"""
    # aggregate and clean data
    df['hurt'] = df['p13a'] + df['p13b'] + df['p13c']
    df_grouped = df.groupby(['p45a', 'p_year']).aggregate({'p1': 'count', 'hurt': 'mean'})
    df_grouped = df_grouped.drop([98, 99, ]).rename(columns={'p1': 'count'})

    # confirm theory that mostly represented cars participate in most accidents
    most_accidents = df_grouped['count'].nlargest(3).reset_index()
    most_accidents['p45a'] = most_accidents['p45a'].map(car_types)
    print(f'Three car types involved in most accidents:\n{most_accidents.to_string(header=False)}\n')

    # find cars u don't want to sit in
    worst_injury_ratio = df_grouped[df_grouped['count'] > 50]['hurt'].nlargest(3).reset_index()
    worst_injury_ratio['p45a'] = worst_injury_ratio['p45a'].map(car_types)
    print(f'Three car types with worst injury ratio:\n{worst_injury_ratio.to_string(header=False)}\n')


def print_stats(df: pd.DataFrame):
    """Print stats about dataset"""
    print(f'Mean rate of death in selected accidents: {df["p13a"].mean() * 100:.2f}%')
    print(f'Mean rate of hard injury in selected accidents: {df["p13b"].mean() * 100:.2f}%')
    print(f'Mean rate of low injury in selected accidents: {df["p13c"].mean() * 100:.2f}%\n')


if __name__ == "__main__":
    df_accidents = get_dataset("accidents.pkl.gz")
    print(f'Number of accidents without drugs or alcohol: {df_accidents.shape[0]}\n')
    plot_car_type(df_accidents, 'fig.pdf', False)
    create_table_by_year(df_accidents)
