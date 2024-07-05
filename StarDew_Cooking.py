import pandas as pd
from IPython.display import display

def to_float(data_frame, columns):
    for ele in columns:
        data_frame[ele] = data_frame[ele].astype(float)
def to_int(data_frame, columns):
    for ele in columns:
        data_frame[ele] = data_frame[ele].astype(int)


spring = pd.read_csv('file_path\\Spring Vegtables.csv')
pd.set_option('display.max_columns', None)

spring.replace('Null', '0', inplace=True, regex=True)
spring.replace('150 (30)', '30', inplace=True, regex=False)
spring.rename(columns={'Catagory' : 'Category'}, inplace=True)


columns = ['Dehydrator Artisan', 'Dehydrator', 'Preserve Jar', 'Keg Artisan', 'Keg', 'Tiller Gold', 'Tiller Silver', 'Tiller Base', 'Tiller Iridium']
to_float(spring, columns)

#print(spring.dtypes)
spring.to_parquet('file_path\\Spring Vegetable.parquet',engine='fastparquet')

summer = pd.read_csv('file_path\\Summer Vegtables.csv')
summer.replace('Null', '0', inplace=True, regex= True)
summer.rename(columns={'Catagory' : 'Category'}, inplace=True)

to_float(summer, columns)

summer['Preserve Jar Artisan'] = summer['Preserve Jar Artisan'].astype(float)

summer.to_parquet('file_path\\Summer Vegetables.parquet',engine='fastparquet')

fall = pd.read_csv('file_path\\Fall Vegtables.csv')
fall.replace('Null', '0', inplace=True, regex= True)
fall.rename(columns={'Catagory' : 'Category'}, inplace= True)

to_float(fall, columns)
fall['Preserve Jar Artisan'] = fall['Preserve Jar Artisan'].astype(float)
#print(fall.dtypes)
fall.to_parquet('file_path\\Fall Vegetables.parquet',engine='fastparquet')

winter = pd.read_csv('file_path\\Project CSVs\\Winter Vegtables.csv')
winter.replace('Null', '0', inplace= True, regex= True)
winter.rename(columns={'Catagory': 'Category'}, inplace= True)

to_float(winter, columns)
winter['Preserve Jar Artisan'] = winter['Preserve Jar Artisan'].astype(float)
#print(winter.dtypes)
winter.to_parquet('file_path\\Winter Vegetables.parquet', engine='fastparquet')

merged_veg = pd.concat([spring, summer, fall, winter])
merged_veg.to_parquet('file_path\\Merged Vegetables.parquet', engine='fastparquet')

fish = pd.read_csv('file_path\\Fish Only.csv')

columns1 = [x for x in fish.columns if x != 'Name']
to_float(fish, columns1)

fish1 = fish[['Base.1', 'Silver.1', 'Gold .1', 'Iridium.1', 'Fisher Base.1', 'Fisher Silver.1', 'Fisher Gold.1', 'Fisher Iridium.1', 'Angler Base.1', 'Angler silver.1',
       'Angler gold.1', 'Angler Iridium.1']]
fish2 = fish[['Base.2', 'Silver.2', 'Gold .2',
       'Iridium.2', 'Fisher Base.2', 'Fisher Silver.2', 'Fisher Gold.2',
       'Fisher Iridium.2', 'Angler Base.2', 'Angler silver.2', 'Angler gold.2',
       'Angler Iridium.2']]
fish = fish[['Name', 'Base', 'Silver', 'Gold ', 'Iridium', 'Fisher Base',
       'Fisher Silver', 'Fisher Gold', 'Fisher Iridium', 'Angler Base',
       'Angler silver', 'Angler gold', 'Angler Iridium']]
fish1 = fish1.rename(columns={'Base.1': 'Base Smoker', 'Silver.1': 'Silver Smoker', 'Gold .1': 'Gold Smoker', 'Iridium.1': 'Iridium Smoker', 'Fisher Base.1': 'Fisher Base Smoker', 'Fisher Silver.1': 'Fisher Silver Smoker', 'Fisher Gold.1': 'Fisher Gold Smoker', 'Fisher Iridium.1': 'Fisher Iridium Smoker', 'Angler Base.1': 'Angler Base Smoker', 'Angler silver.1': 'Angler Silver Smoker',
       'Angler gold.1': 'Angler Gold Smoker', 'Angler Iridium.1': 'Angler Iridium Smoker'})
fish2 = fish2.rename(columns={'Base.2': 'Base Artisan Smoker', 'Silver.2': 'Silver Artisan Smoker', 'Gold .2': 'Gold Artisan Smoker',
       'Iridium.2': 'Iridium Artisan Smoker', 'Fisher Base.2': 'Fisher Base Artisan Smoker', 'Fisher Silver.2': 'Fisher Silver Artisan Smoker', 'Fisher Gold.2': 'Fisher Gold Artisan Smoker',
       'Fisher Iridium.2': 'Fisher Iridium Artisan Smoker', 'Angler Base.2': 'Angler Base Artisan Smoker', 'Angler silver.2': 'Angler Silver Artisan Smoker', 'Angler gold.2': 'Angler Gold Artisan Smoker',
       'Angler Iridium.2': 'Angler Iridium Artisan Smoker'})

display(fish)
display(fish1)
display(fish2)

fish.to_parquet('file_path\\Fish Only.parquet', engine='fastparquet')
fish1.to_parquet('file_path\\Fish Smoker.parquet', engine='fastparquet')
fish2.to_parquet('file_path\\Fish Artisan Smoker.parquet', engine='fastparquet')
