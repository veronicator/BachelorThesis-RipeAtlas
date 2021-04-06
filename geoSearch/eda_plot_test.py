#%%
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import os
from datetime import datetime

# Comment this if the data visualisations doesn't work on your side
#%matplotlib inline

# palette: deep, muted, pastel, bright, dark, and colorblind

path_dir = '/home/veronica/Documents/ripe_geo_results/'
plot_dir = path_dir + 'eda_results/'

sns.set_theme()
sns.set(rc={'figure.figsize': (15, 15)})

df_ping_4 = pd.read_csv(path_dir + 'ping_tab_4.csv')    #return pandas.DataFrame
print(df_ping_4.head())     #.tail(n=row to select)/.head(n)
#print("get", df_ping_4.get('asn_src'))
#print("desc", df_ping_4['asn_src'].describe())
print('num asn_src_v4:', df_ping_4['asn_src'].nunique()) # Count distinct observations over requested axis.
print('num asn_dest_v4:', df_ping_4['asn_dest'].nunique())

#format_date = datetime.fromtimestamp(df_ping_4['timestamp'])
df_ping_4['timestamp'] = pd.to_datetime(arg=df_ping_4['timestamp'], unit='s')
#for row in df_ping_4['timestamp']:
 #   df_ping_4['timestamp'] = datetime.fromtimestamp(row)
    #print(row)
    #df_ping_4[row]['timestamp'] = datetime.fromtimestamp(df_ping_4[row]['timestamp'])
print(df_ping_4.head())

sns.scatterplot(data=df_ping_4, x="timestamp", y="rtt_min", palette="deep", s=5)#, hue="ip_src")  #, hue="asn_src"
plt.xlabel('Time (GMT)')
plt.ylabel('RTT min (ms)')
plt.savefig(path_dir + 'rtt_ping_v4.png')
plt.figure()

p4 = sns.relplot(data=df_ping_4, x="timestamp", y="rtt_min", col="asn_src", kind="scatter", palette="deep", hue="asn_dest", legend="full", s=5)
p4.set_axis_labels('Time (GMT)', 'RTT min (ms)')
#plt.xlabel('Time (GMT)')
#plt.ylabel('RTT min (ms)')
plt.savefig(path_dir + 'ping_asn_src_v4.png')
plt.figure()

p4 = sns.relplot(data=df_ping_4, x="timestamp", y="rtt_min", col="asn_dest", kind="scatter", palette="deep", legend="full", s=5)
p4.set_axis_labels('Time (GMT)', 'RTT min (ms)')
#plt.xlabel('Time (GMT)')
#plt.ylabel('RTT min (ms)')
plt.savefig(path_dir + 'ping_asn_dest_v4.png')
plt.figure()

df_ping_6 = pd.read_csv(path_dir + 'ping_tab_6.csv')
#print(df_ping_6.head())
print('num asn_src_v6:', df_ping_6['asn_src'].nunique()) # Count distinct observations over requested axis.
print('num asn_dest_v6:', df_ping_6['asn_dest'].nunique())

df_ping_6['timestamp'] = pd.to_datetime(arg=df_ping_6['timestamp'], unit='s')

sns.scatterplot(data=df_ping_6, x="timestamp", y="rtt_min", palette="deep", s=5)  # hue="asn_src",
plt.xlabel('Time (GMT)')
plt.ylabel('RTT min (ms)')
plt.savefig(path_dir + 'rtt_ping_v6.png')
plt.figure()

p6 = sns.relplot(data=df_ping_6, x="timestamp", y="rtt_min", col="asn_src", kind="scatter", palette="deep", hue="asn_dest", legend="full", s=5)
p6.set_axis_labels('Time (GMT)', 'RTT min (ms)')
#plt.xlabel('Time (GMT)')
#plt.ylabel('RTT min (ms)')
plt.savefig(path_dir + 'ping_asn_src_v6.png')
plt.figure()

p6 = sns.relplot(data=df_ping_6, x="timestamp", y="rtt_min", col="asn_dest", kind="scatter", palette="deep", hue="asn_src", legend="full", s=5)
p6.set_axis_labels('Time (GMT)', 'RTT min (ms)')
#plt.xlabel('Time (GMT)')
#plt.ylabel('RTT min (ms)')
plt.savefig(path_dir + 'ping_asn_dest_v6.png')
plt.figure()


"""
plt.style.use('bmh')


df = pd.read_csv('../ripe_geo_results/ping_tab_4.csv')
df.head()

print(df.info())

# df.count() does not include NaN values
df2 = df[[column for column in df if df[column].count() / len(df) >= 0.3]]
#del df2['Id']
print("List of dropped columns:", end=" ")
for c in df.columns:
    if c not in df2.columns:
        print(c, end=", ")
print('\n')
df = df2

print(df['rtt_min'].describe())
plt.figure(figsize=(9, 8))
sns.distplot(df['rtt_min'], color='g', bins=100, hist_kws={'alpha': 0.4})


print(list(set(df.dtypes.tolist())))

df_num = df.select_dtypes(include = ['float64', 'int64'])
print(df_num.head())

df_num.hist(figsize=(16, 20), bins=50, xlabelsize=8, ylabelsize=8); # ; avoid having the matplotlib verbose informations

"""

"""
ping_grouped = df_ping_4.groupby(by='asn_src')
for group, df_group in ping_grouped:
    #print(df_group)
    for row_index, row in df_group.iterrows():
        print(row['asn_src'])
    #print(d)
    df = pd.DataFrame(data=d)
    sns.scatterplot(data=df, x='timestamp', y='rtt_min', palette='deep')
    plt.figure()
"""
# %%
